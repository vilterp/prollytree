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
            runtime: Arc::new(runtime),
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
            runtime: Arc::new(runtime),
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
    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
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
                    Ok(_) => Some(()),
                    Err(_) => None,
                }
            }
            Err(_) => None,
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

    // Note: These tests require AWS credentials and a real S3 bucket
    // They are disabled by default and should be run manually
    #[test]
    #[ignore]
    fn test_s3_basic_operations() {
        // This test would require AWS credentials and a bucket
        // Example implementation:
        // let config = aws_config::load_from_env().await;
        // let client = Client::new(&config);
        // let mut storage = S3NodeStorage::<32>::new(client, "test-bucket".to_string(), "test/".to_string());
        //
        // let node = create_test_node();
        // let hash = node.get_hash();
        //
        // assert!(storage.insert_node(hash.clone(), node.clone()).is_some());
        // let retrieved = storage.get_node_by_hash(&hash);
        // assert!(retrieved.is_some());
    }
}
