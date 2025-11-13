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
use aws_sdk_s3::Client as S3Client;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

/// S3-backed storage for ProllyTree nodes
///
/// This storage implementation uses AWS S3 (or S3-compatible storage) as the
/// persistent storage backend, with an LRU cache for frequently accessed nodes
/// to improve performance.
///
/// # Architecture
///
/// - Nodes are stored as S3 objects with keys based on their hash
/// - Configs are stored with a special prefix
/// - An LRU cache reduces S3 API calls for frequently accessed nodes
/// - All S3 operations are async but wrapped in sync interface using tokio runtime
#[derive(Debug)]
pub struct S3NodeStorage<const N: usize> {
    client: Arc<S3Client>,
    bucket: String,
    prefix: String,
    cache: Arc<Mutex<LruCache<ValueDigest<N>, ProllyNode<N>>>>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl<const N: usize> Clone for S3NodeStorage<N> {
    fn clone(&self) -> Self {
        S3NodeStorage {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1000).unwrap()))),
            runtime: self.runtime.clone(),
        }
    }
}

impl<const N: usize> S3NodeStorage<N> {
    /// Create a new S3NodeStorage instance with default configuration
    ///
    /// This will use the default AWS credential chain and region configuration.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The S3 bucket name
    /// * `prefix` - Optional prefix for all keys (e.g., "prolly-trees/my-tree/")
    pub async fn new(bucket: String, prefix: Option<String>) -> Result<Self, String> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = S3Client::new(&config);

        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| format!("Failed to create tokio runtime: {}", e))?;

        Ok(S3NodeStorage {
            client: Arc::new(client),
            bucket,
            prefix: prefix.unwrap_or_default(),
            cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1000).unwrap()))),
            runtime: Arc::new(runtime),
        })
    }

    /// Create S3NodeStorage with custom cache size
    pub async fn with_cache_size(
        bucket: String,
        prefix: Option<String>,
        cache_size: usize,
    ) -> Result<Self, String> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = S3Client::new(&config);

        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| format!("Failed to create tokio runtime: {}", e))?;

        let cache_size = NonZeroUsize::new(cache_size).unwrap_or(NonZeroUsize::new(1000).unwrap());

        Ok(S3NodeStorage {
            client: Arc::new(client),
            bucket,
            prefix: prefix.unwrap_or_default(),
            cache: Arc::new(Mutex::new(LruCache::new(cache_size))),
            runtime: Arc::new(runtime),
        })
    }

    /// Create S3NodeStorage with custom AWS config
    pub async fn with_config(
        bucket: String,
        prefix: Option<String>,
        aws_config: &aws_config::SdkConfig,
    ) -> Result<Self, String> {
        let client = S3Client::new(aws_config);

        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| format!("Failed to create tokio runtime: {}", e))?;

        Ok(S3NodeStorage {
            client: Arc::new(client),
            bucket,
            prefix: prefix.unwrap_or_default(),
            cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1000).unwrap()))),
            runtime: Arc::new(runtime),
        })
    }

    /// Create a blocking version from async
    pub fn new_blocking(bucket: String, prefix: Option<String>) -> Result<Self, String> {
        // Create a separate runtime for initialization
        let init_runtime = tokio::runtime::Runtime::new()
            .map_err(|e| format!("Failed to create tokio runtime: {}", e))?;

        let (client, main_runtime) = init_runtime.block_on(async {
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = S3Client::new(&config);

            let main_runtime = tokio::runtime::Runtime::new()
                .map_err(|e| format!("Failed to create tokio runtime: {}", e))?;

            Ok::<_, String>((client, main_runtime))
        })?;

        Ok(S3NodeStorage {
            client: Arc::new(client),
            bucket,
            prefix: prefix.unwrap_or_default(),
            cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1000).unwrap()))),
            runtime: Arc::new(main_runtime),
        })
    }

    /// Generate S3 key for a node
    fn node_key(&self, hash: &ValueDigest<N>) -> String {
        let hash_hex: String = hash.0.iter().map(|b| format!("{:02x}", b)).collect();
        format!("{}nodes/{}", self.prefix, hash_hex)
    }

    /// Generate S3 key for a config
    fn config_key(&self, key: &str) -> String {
        format!("{}config/{}", self.prefix, key)
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
        let client = self.client.clone();
        let bucket = self.bucket.clone();

        let result = self.runtime.block_on(async {
            match client.get_object().bucket(&bucket).key(&key).send().await {
                Ok(output) => {
                    let bytes = output.body.collect().await.ok()?.into_bytes();
                    bincode::deserialize::<ProllyNode<N>>(&bytes).ok()
                }
                Err(_) => None,
            }
        });

        if let Some(mut node) = result {
            // Reset transient flags
            node.split = false;
            node.merged = false;

            // Update cache
            self.cache.lock().unwrap().put(hash.clone(), node.clone());

            Some(node)
        } else {
            None
        }
    }

    fn insert_node(&mut self, hash: ValueDigest<N>, node: ProllyNode<N>) -> Option<()> {
        // Update cache
        self.cache.lock().unwrap().put(hash.clone(), node.clone());

        // Serialize node
        let data = bincode::serialize(&node).ok()?;

        // Store in S3
        let key = self.node_key(&hash);
        let client = self.client.clone();
        let bucket = self.bucket.clone();

        self.runtime.block_on(async {
            client
                .put_object()
                .bucket(&bucket)
                .key(&key)
                .body(data.into())
                .send()
                .await
                .ok()
                .map(|_| ())
        })
    }

    fn delete_node(&mut self, hash: &ValueDigest<N>) -> Option<()> {
        // Remove from cache
        self.cache.lock().unwrap().pop(hash);

        // Delete from S3
        let key = self.node_key(hash);
        let client = self.client.clone();
        let bucket = self.bucket.clone();

        self.runtime.block_on(async {
            client
                .delete_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .ok()
                .map(|_| ())
        })
    }

    fn save_config(&self, key: &str, config: &[u8]) {
        let s3_key = self.config_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let config_data = config.to_vec();

        let _ = self.runtime.block_on(async {
            client
                .put_object()
                .bucket(&bucket)
                .key(&s3_key)
                .body(config_data.into())
                .send()
                .await
        });
    }

    fn get_config(&self, key: &str) -> Option<Vec<u8>> {
        let s3_key = self.config_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();

        self.runtime.block_on(async {
            match client
                .get_object()
                .bucket(&bucket)
                .key(&s3_key)
                .send()
                .await
            {
                Ok(output) => {
                    let bytes = output.body.collect().await.ok()?.into_bytes();
                    Some(bytes.to_vec())
                }
                Err(_) => None,
            }
        })
    }
}

/// Batch operations for S3NodeStorage
impl<const N: usize> S3NodeStorage<N> {
    /// Insert multiple nodes efficiently
    ///
    /// Note: S3 doesn't have native batch operations, but this method
    /// can still benefit from caching multiple nodes at once
    pub fn batch_insert_nodes(
        &mut self,
        nodes: Vec<(ValueDigest<N>, ProllyNode<N>)>,
    ) -> Result<(), String> {
        let mut cache = self.cache.lock().unwrap();

        for (hash, node) in nodes {
            // Update cache first
            cache.put(hash.clone(), node.clone());

            // Serialize and store in S3
            let data =
                bincode::serialize(&node).map_err(|e| format!("Serialization failed: {}", e))?;

            let key = self.node_key(&hash);
            let client = self.client.clone();
            let bucket = self.bucket.clone();

            self.runtime.block_on(async {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(data.into())
                    .send()
                    .await
                    .map_err(|e| format!("S3 put failed: {}", e))
            })?;
        }

        Ok(())
    }

    /// Delete multiple nodes efficiently
    pub fn batch_delete_nodes(&mut self, hashes: &[ValueDigest<N>]) -> Result<(), String> {
        let mut cache = self.cache.lock().unwrap();

        for hash in hashes {
            // Remove from cache
            cache.pop(hash);

            // Delete from S3
            let key = self.node_key(hash);
            let client = self.client.clone();
            let bucket = self.bucket.clone();

            self.runtime.block_on(async {
                client
                    .delete_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| format!("S3 delete failed: {}", e))
            })?;
        }

        Ok(())
    }

    /// Get the bucket name
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Get the prefix
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TreeConfig;

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

    #[tokio::test]
    #[ignore] // Requires AWS credentials and S3 bucket
    async fn test_s3_basic_operations() {
        // This test requires:
        // - AWS credentials configured
        // - A test bucket available
        // Use environment variable TEST_S3_BUCKET to specify bucket

        let bucket = std::env::var("TEST_S3_BUCKET")
            .unwrap_or_else(|_| "test-prollytree-bucket".to_string());

        let mut storage = S3NodeStorage::<32>::new(bucket, Some("test/".to_string()))
            .await
            .unwrap();

        let node = create_test_node();
        let hash = node.get_hash();

        // Test insert
        assert!(storage.insert_node(hash.clone(), node.clone()).is_some());

        // Test get
        let retrieved = storage.get_node_by_hash(&hash);
        assert!(retrieved.is_some());

        let retrieved_node = retrieved.unwrap();
        assert_eq!(retrieved_node.keys, node.keys);
        assert_eq!(retrieved_node.values, node.values);
        assert_eq!(retrieved_node.is_leaf, node.is_leaf);

        // Test delete
        assert!(storage.delete_node(&hash).is_some());
        assert!(storage.get_node_by_hash(&hash).is_none());
    }

    #[tokio::test]
    #[ignore] // Requires AWS credentials and S3 bucket
    async fn test_config_operations() {
        let bucket = std::env::var("TEST_S3_BUCKET")
            .unwrap_or_else(|_| "test-prollytree-bucket".to_string());

        let storage = S3NodeStorage::<32>::new(bucket, Some("test/".to_string()))
            .await
            .unwrap();

        let config_data = b"test config data";
        storage.save_config("test_key", config_data);

        let retrieved = storage.get_config("test_key");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), config_data);

        // Test non-existent config
        assert!(storage.get_config("non_existent").is_none());
    }
}
