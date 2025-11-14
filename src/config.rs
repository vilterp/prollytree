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
use crate::encoding::EncodingType;
use schemars::schema::RootSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TreeConfig<const N: usize> {
    pub base: u64,
    pub modulus: u64,
    pub min_chunk_size: usize,
    pub max_chunk_size: usize,
    pub pattern: u64,
    pub root_hash: Option<ValueDigest<N>>,
    pub key_schema: Option<RootSchema>,
    pub value_schema: Option<RootSchema>,
    pub encode_types: Vec<EncodingType>,
}

impl<const N: usize> Default for TreeConfig<N> {
    fn default() -> Self {
        TreeConfig {
            base: 257,
            modulus: 1_000_000_007,
            // Optimized for remote storage (S3, etc.) with higher latency
            // Larger nodes reduce roundtrips at the cost of more data per fetch
            min_chunk_size: 8,          // Minimum 8 entries per node
            max_chunk_size: 256 * 1024, // Max 256K entries (rarely hit in practice)
            pattern: 0b111111,          // 63 - split probability ~1.5% (~64-128 entries/node)
            root_hash: None,
            key_schema: None,
            value_schema: None,
            encode_types: vec![],
        }
    }
}
