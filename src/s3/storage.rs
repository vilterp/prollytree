/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use crate::digest::ValueDigest;
use crate::node::ProllyNode;
use crate::storage::NodeStorage;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use lru::LruCache;
use std::mem::ManuallyDrop;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

const CONFIG_PREFIX: &str = "config:";
const NODE_PREFIX: &str = "node:";

/// S3-backed storage for ProllyTree nodes
///
/// This storage implementation uses AWS S3 as the persistent storage backend,
/// with an LRU cache for frequently accessed nodes to improve performance.
#[derive(Debug)]
pub struct S3NodeStorage<const N: usize> {
    client: Arc<Client>,
    bucket: String,
    prefix: String,
    cache: Arc<Mutex<LruCache<ValueDigest<N>, ProllyNode<N>>>>,
    /// Runtime wrapped in ManuallyDrop to prevent drop issues in async contexts
    /// We manually manage its lifecycle to avoid "cannot drop runtime in async context" panics
    runtime: ManuallyDrop<Option<Arc<tokio::runtime::Runtime>>>,
}

impl<const N: usize> Clone for S3NodeStorage<N> {
    fn clone(&self) -> Self {
        S3NodeStorage {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1000).unwrap()))),
            runtime: ManuallyDrop::new((*self.runtime).clone()),
        }
    }
}

impl<const N: usize> Drop for S3NodeStorage<N> {
    fn drop(&mut self) {
        // Since runtime is ManuallyDrop, we don't drop it automatically
        // This prevents the "cannot drop runtime in async context" panic
        // The runtime will be cleaned up when the process exits
    }
}

impl<const N: usize> S3NodeStorage<N> {
    /// Create a new S3NodeStorage instance with default cache size
    ///
    /// # Arguments
    ///
    /// * `client` - AWS S3 client
    /// * `bucket` - S3 bucket name
    /// * `prefix` - Key prefix for all objects (e.g., "prollytree/")
    pub fn new(client: Client, bucket: String, prefix: String) -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        S3NodeStorage {
            client: Arc::new(client),
            bucket,
            prefix,
            cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1000).unwrap()))),
            runtime: ManuallyDrop::new(Some(Arc::new(runtime))),
        }
    }

    /// Create S3NodeStorage with custom cache size
    pub fn with_cache_size(
        client: Client,
        bucket: String,
        prefix: String,
        cache_size: usize,
    ) -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        S3NodeStorage {
            client: Arc::new(client),
            bucket,
            prefix,
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(cache_size).unwrap_or(NonZeroUsize::new(1000).unwrap()),
            ))),
            runtime: ManuallyDrop::new(Some(Arc::new(runtime))),
        }
    }

    /// Create a key for storing a node in S3
    fn node_key(&self, hash: &ValueDigest<N>) -> String {
        format!("{}{}{:x}", self.prefix, NODE_PREFIX, hash)
    }

    /// Create a key for storing config in S3
    fn config_key(&self, key: &str) -> String {
        format!("{}{}{}", self.prefix, CONFIG_PREFIX, key)
    }

    /// Helper to run async operations synchronously
    /// This handles both cases: when called from within an async context and when not
    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        // Check if we're already inside a tokio runtime
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                // We're inside a runtime, use block_in_place to avoid nested runtime error
                tokio::task::block_in_place(|| handle.block_on(future))
            }
            Err(_) => {
                // We're not in a runtime, use our own
                self.runtime
                    .as_ref()
                    .expect("Runtime should be available")
                    .block_on(future)
            }
        }
    }
}

impl<const N: usize> NodeStorage<N> for S3NodeStorage<N> {
    fn get_node_by_hash(&self, hash: &ValueDigest<N>) -> Option<ProllyNode<N>> {
        // Check cache first
        if let Some(node) = self.cache.lock().unwrap().get(hash) {
            return Some(node.clone());
        }

        // Fetch from S3
        let key = self.node_key(hash);
        let result = self.block_on(async {
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
        });

        match result {
            Ok(output) => {
                let data = self.block_on(async { output.body.collect().await });

                match data {
                    Ok(bytes) => {
                        match bincode::deserialize::<ProllyNode<N>>(&bytes.into_bytes()) {
                            Ok(mut node) => {
                                // Reset transient flags
                                node.split = false;
                                node.merged = false;

                                // Update cache
                                self.cache.lock().unwrap().put(hash.clone(), node.clone());

                                Some(node)
                            }
                            Err(_) => None,
                        }
                    }
                    Err(_) => None,
                }
            }
            Err(_) => None,
        }
    }

    fn insert_node(&mut self, hash: ValueDigest<N>, node: ProllyNode<N>) -> Option<()> {
        // Update cache
        self.cache.lock().unwrap().put(hash.clone(), node.clone());

        // Serialize and store in S3
        match bincode::serialize(&node) {
            Ok(data) => {
                let key = self.node_key(&hash);
                let result = self.block_on(async {
                    self.client
                        .put_object()
                        .bucket(&self.bucket)
                        .key(&key)
                        .body(ByteStream::from(data))
                        .send()
                        .await
                });

                match result {
                    Ok(_) => {
                        #[cfg(feature = "tracing")]
                        tracing::debug!("Successfully wrote node to S3: {}", key);
                        Some(())
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::error!("Failed to write node to S3 ({}): {:?}", key, e);
                        #[cfg(not(feature = "tracing"))]
                        eprintln!("S3 write error for key {}: {:?}", key, e);
                        None
                    }
                }
            }
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::error!("Failed to serialize node: {:?}", e);
                #[cfg(not(feature = "tracing"))]
                eprintln!("Node serialization error: {:?}", e);
                None
            }
        }
    }

    fn delete_node(&mut self, hash: &ValueDigest<N>) -> Option<()> {
        // Remove from cache
        self.cache.lock().unwrap().pop(hash);

        // Delete from S3
        let key = self.node_key(hash);
        let result = self.block_on(async {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
        });

        match result {
            Ok(_) => Some(()),
            Err(_) => None,
        }
    }

    fn save_config(&self, key: &str, config: &[u8]) {
        let s3_key = self.config_key(key);
        let _ = self.block_on(async {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .body(ByteStream::from(config.to_vec()))
                .send()
                .await
        });
    }

    fn get_config(&self, key: &str) -> Option<Vec<u8>> {
        let s3_key = self.config_key(key);
        let result = self.block_on(async {
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .send()
                .await
        });

        match result {
            Ok(output) => {
                let data = self.block_on(async { output.body.collect().await });

                match data {
                    Ok(bytes) => Some(bytes.into_bytes().to_vec()),
                    Err(_) => None,
                }
            }
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TreeConfig;
    use crate::digest::ValueDigest;
    use crate::node::ProllyNode;
    use crate::tree::{ProllyTree, Tree};

    fn create_test_node<const N: usize>() -> ProllyNode<N> {
        let config: TreeConfig<N> = TreeConfig::default();
        ProllyNode {
            keys: vec![b"key1".to_vec(), b"key2".to_vec()],
            key_schema: config.key_schema.clone(),
            values: vec![b"value1".to_vec(), b"value2".to_vec()],
            value_schema: config.value_schema.clone(),
            is_leaf: true,
            level: 0,
            base: config.base,
            modulus: config.modulus,
            min_chunk_size: config.min_chunk_size,
            max_chunk_size: config.max_chunk_size,
            pattern: config.pattern,
            split: false,
            merged: false,
            encode_types: Vec::new(),
            encode_values: Vec::new(),
        }
    }

    // Note: These tests require LocalStack or AWS S3 to be available
    #[test]
    #[ignore] // Run with: cargo test --features s3_storage test_s3_basic_operations -- --ignored --nocapture
    fn test_s3_basic_operations() {
        use tokio::runtime::Runtime;

        let rt = Runtime::new().unwrap();

        rt.block_on(async {
            // Configure for LocalStack
            let endpoint_url = std::env::var("S3_ENDPOINT_URL")
                .unwrap_or_else(|_| "http://localhost:4566".to_string());
            let bucket = std::env::var("S3_BUCKET")
                .unwrap_or_else(|_| "prollytree-test".to_string());
            let region = std::env::var("AWS_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string());

            println!("Connecting to S3:");
            println!("  Endpoint: {}", endpoint_url);
            println!("  Bucket: {}", bucket);
            println!("  Region: {}", region);

            // Create AWS config
            let config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new(region))
                .endpoint_url(&endpoint_url);

            let sdk_config = config_loader.load().await;

            let client = aws_sdk_s3::Client::new(&sdk_config);

            // Test bucket exists
            println!("Testing bucket access...");
            let bucket_result = client.head_bucket()
                .bucket(&bucket)
                .send()
                .await;

            match bucket_result {
                Ok(_) => println!("✓ Bucket '{}' is accessible", bucket),
                Err(e) => {
                    eprintln!("✗ Bucket '{}' not accessible: {:?}", bucket, e);
                    panic!("Cannot access S3 bucket. Make sure LocalStack is running and bucket exists.");
                }
            }

            // Create storage
            let mut storage = S3NodeStorage::<32>::new(
                client,
                bucket.clone(),
                "test/rust-test/".to_string()
            );

            // Create a test node
            let node = create_test_node();
            let hash = node.get_hash();

            println!("Inserting test node with hash: {:x}", hash);

            // Insert node
            let insert_result = storage.insert_node(hash.clone(), node.clone());
            assert!(insert_result.is_some(), "Failed to insert node to S3");
            println!("✓ Node inserted successfully");

            // Retrieve node
            println!("Retrieving node from S3...");
            let retrieved = storage.get_node_by_hash(&hash);
            assert!(retrieved.is_some(), "Failed to retrieve node from S3");
            println!("✓ Node retrieved successfully");

            let retrieved_node = retrieved.unwrap();
            assert_eq!(node.keys, retrieved_node.keys, "Retrieved node keys don't match");
            assert_eq!(node.values, retrieved_node.values, "Retrieved node values don't match");
            println!("✓ Retrieved node data matches original");

            // Test with ProllyTree operations
            println!("\nTesting with ProllyTree...");
            let config = TreeConfig::default();
            let storage2 = S3NodeStorage::<32>::new(
                storage.client.as_ref().clone(),
                bucket,
                "test/tree-test/".to_string()
            );

            let mut tree = crate::tree::ProllyTree::new(storage2, config);

            println!("Inserting key-value pairs...");
            tree.insert(b"key1".to_vec(), b"value1".to_vec());
            tree.insert(b"key2".to_vec(), b"value2".to_vec());
            tree.insert(b"key3".to_vec(), b"value3".to_vec());

            println!("✓ Inserted 3 key-value pairs");

            // Verify retrieval
            println!("Verifying data retrieval...");

            // Helper to extract value from node
            let get_value = |node: &ProllyNode<32>, key: &[u8]| -> Vec<u8> {
                node.keys.iter()
                    .position(|k| k == key)
                    .map(|idx| node.values[idx].clone())
                    .expect("Key not found in node")
            };

            let node1 = tree.find(&b"key1".to_vec()).expect("key1 not found");
            assert_eq!(get_value(&node1, b"key1"), b"value1".to_vec());

            let node2 = tree.find(&b"key2".to_vec()).expect("key2 not found");
            assert_eq!(get_value(&node2, b"key2"), b"value2".to_vec());

            let node3 = tree.find(&b"key3".to_vec()).expect("key3 not found");
            assert_eq!(get_value(&node3, b"key3"), b"value3".to_vec());

            println!("✓ All values retrieved correctly");

            println!("\n✓ All S3 storage tests passed!");
        });
    }

    #[test]
    #[ignore] // Run with: cargo test --features s3_storage test_s3_tree_persistence -- --ignored --nocapture
    fn test_s3_tree_persistence() {
        use tokio::runtime::Runtime;

        let rt = Runtime::new().unwrap();

        rt.block_on(async {
            let endpoint_url = std::env::var("S3_ENDPOINT_URL")
                .unwrap_or_else(|_| "http://localhost:4566".to_string());
            let bucket =
                std::env::var("S3_BUCKET").unwrap_or_else(|_| "prollytree-test".to_string());
            let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

            println!("Testing tree persistence to S3");
            println!("  Endpoint: {}", endpoint_url);
            println!("  Bucket: {}", bucket);

            // Create client
            let config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new(region));
            let sdk_config = config_loader.load().await;
            let s3_config_builder =
                aws_sdk_s3::config::Builder::from(&sdk_config).endpoint_url(&endpoint_url);
            let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

            // Create tree with S3 storage
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let prefix = format!("test/persistence-test-{}/", timestamp);
            println!("Using prefix: {}", prefix);

            let storage = S3NodeStorage::<32>::new(client.clone(), bucket.clone(), prefix.clone());

            let config = TreeConfig::default();
            let mut tree1 = ProllyTree::new(storage, config.clone());

            // Insert data
            println!("Inserting 10 key-value pairs...");
            for i in 0..10 {
                tree1.insert(
                    format!("key{}", i).into_bytes(),
                    format!("value{}", i).into_bytes(),
                );
            }

            let root_hash = tree1.get_root_hash().unwrap();
            println!("Root hash: {:x}", root_hash);

            // Check S3 to see what was actually written
            println!("\nListing objects in S3 with prefix '{}'...", prefix);
            let list_result = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .send()
                .await;

            match list_result {
                Ok(output) => {
                    let count = output.contents().len();
                    println!("✓ Found {} objects in S3:", count);
                    for obj in output.contents() {
                        println!("  - {}", obj.key().unwrap_or("unknown"));
                    }
                    assert!(
                        count > 0,
                        "Expected nodes to be written to S3 but found none!"
                    );
                }
                Err(e) => {
                    panic!("Failed to list S3 objects: {:?}", e);
                }
            }

            // Create new tree instance from same storage to verify persistence
            println!("\nCreating new tree instance from persisted data...");
            let storage2 = S3NodeStorage::<32>::new(client, bucket, prefix);

            let mut config2 = TreeConfig::default();
            config2.root_hash = Some(root_hash);

            let tree2 = ProllyTree::load_from_storage(storage2, config2);
            assert!(tree2.is_some(), "Failed to load tree from S3 storage");

            let tree2 = tree2.unwrap();
            println!("✓ Tree loaded from S3");

            // Verify data
            println!("Verifying persisted data...");

            // Helper to extract value from node
            let get_value = |node: &ProllyNode<32>, key: &[u8]| -> Vec<u8> {
                node.keys
                    .iter()
                    .position(|k| k.as_slice() == key)
                    .map(|idx| node.values[idx].clone())
                    .expect("Key not found in node")
            };

            for i in 0..10 {
                let key = format!("key{}", i).into_bytes();
                let expected = format!("value{}", i).into_bytes();
                let node = tree2.find(&key).expect(&format!("key{} not found", i));
                let actual = get_value(&node, &key);
                assert_eq!(actual, expected, "Value mismatch for key{}", i);
            }
            println!("✓ All 10 values verified from persisted tree");

            println!("\n✓ S3 persistence test passed!");
        });
    }
}
