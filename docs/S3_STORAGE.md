# S3 Storage Backend for ProllyTree

This document describes the S3 storage backend for ProllyTree, which enables cloud-native persistence of prolly trees in AWS S3 or S3-compatible storage systems.

## Overview

The S3 storage backend stores ProllyTree nodes as objects in Amazon S3, providing:

- **Cloud-native persistence**: Store your prolly trees in AWS S3 for durability and availability
- **Distributed access**: Multiple systems can access the same tree data from S3
- **Scalable storage**: Leverage S3's unlimited storage capacity
- **Content-addressed nodes**: Efficient deduplication and sharing of tree nodes
- **LRU caching**: Local cache reduces S3 API calls for frequently accessed nodes

## Features

### Rust API

The S3 storage backend implements the `NodeStorage` trait and can be used wherever other storage backends are used.

```rust
use prollytree::{ProllyTree, TreeConfig, storage::S3NodeStorage};

// Create S3 storage (requires tokio runtime)
let storage = S3NodeStorage::new_blocking(
    "my-bucket".to_string(),
    Some("prolly-trees/".to_string())
)?;

// Create tree with S3 storage
let config = TreeConfig::default();
let mut tree = ProllyTree::new(storage, config);

// Use the tree normally
tree.insert(b"key".to_vec(), b"value".to_vec());
let value = tree.find(&b"key".to_vec());
```

### Python API

The S3 storage backend is fully exposed in Python bindings:

```python
from prollytree import ProllyTree

# Create S3-backed tree
tree = ProllyTree(
    storage_type="s3",
    bucket="my-bucket",
    prefix="prolly-trees/"  # Optional
)

# Use normally
tree.insert(b"key", b"value")
value = tree.find(b"key")
```

## Prerequisites

### AWS Setup

1. **AWS Credentials**: Configure AWS credentials using one of these methods:
   - Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
   - AWS credentials file: `~/.aws/credentials`
   - IAM role (when running on EC2/ECS/Lambda)

2. **S3 Bucket**: Create an S3 bucket with appropriate permissions:
   - `s3:GetObject` - Read nodes from S3
   - `s3:PutObject` - Write nodes to S3
   - `s3:DeleteObject` - Delete nodes from S3

3. **Region Configuration**: Set AWS region via:
   - Environment variable: `AWS_REGION` or `AWS_DEFAULT_REGION`
   - AWS config file: `~/.aws/config`

### Build Configuration

#### Rust

Add the S3 storage feature to your `Cargo.toml`:

```toml
[dependencies]
prollytree = { version = "0.3.2-beta", features = ["s3_storage"] }
```

#### Python

Build the Python package with S3 support:

```bash
cd python
./build_python.sh --features "python s3_storage"
pip install -e .
```

Or install from the built wheel:

```bash
cd python
./build_python.sh --features "python s3_storage"
pip install ../target/wheels/prollytree-*.whl
```

## Usage Examples

### Basic Usage

```python
import os
from prollytree import ProllyTree

# Set up AWS credentials and region
os.environ["AWS_REGION"] = "us-east-1"

# Create tree
tree = ProllyTree(
    storage_type="s3",
    bucket="my-bucket",
    prefix="my-app/trees/"
)

# Insert data
tree.insert(b"user:1", b"Alice")
tree.insert(b"user:2", b"Bob")

# Query data
name = tree.find(b"user:1")
print(name)  # b"Alice"

# Tree properties
print(f"Size: {tree.size()}")
print(f"Root hash: {tree.get_root_hash().hex()}")
```

### Batch Operations

```python
# Batch insert for better performance
items = [
    (f"product:{i}".encode(), f"Product {i}".encode())
    for i in range(1000)
]
tree.insert_batch(items)
```

### Custom Configuration

```python
from prollytree import ProllyTree, TreeConfig

config = TreeConfig(
    base=8,
    modulus=128,
    min_chunk_size=2,
    max_chunk_size=8192
)

tree = ProllyTree(
    storage_type="s3",
    bucket="my-bucket",
    prefix="custom/",
    config=config
)
```

### Comparing Trees

Trees can be compared using their cryptographic root hashes, which serve as content fingerprints:

```python
# Create two trees
tree1 = ProllyTree(storage_type="s3", bucket="my-bucket", prefix="tree1/")
tree2 = ProllyTree(storage_type="s3", bucket="my-bucket", prefix="tree2/")

# Insert different data
tree1.insert(b"a", b"1")
tree2.insert(b"a", b"1")
tree2.insert(b"b", b"2")

# Compare via root hashes
if tree1.get_root_hash() == tree2.get_root_hash():
    print("Trees are identical")
else:
    print("Trees differ")
```

**Note:** The basic ProllyTree provides hash-based comparison. For detailed diff operations 
(added/removed/modified keys), use the `VersionedKvStore` which provides Git-like versioning 
and diff functionality. The S3 storage backend works seamlessly with VersionedKvStore for 
version-controlled, diffable key-value storage in S3.

## Storage Structure

The S3 storage backend organizes data as follows:

```
bucket/
├── prefix/
│   ├── nodes/
│   │   ├── <hash1>           # Serialized node
│   │   ├── <hash2>           # Serialized node
│   │   └── ...
│   └── config/
│       ├── <config_key1>     # Tree configuration
│       └── <config_key2>     # Tree configuration
```

- **Nodes**: Stored under `{prefix}nodes/{hash}` where `hash` is the hex-encoded node hash
- **Configs**: Stored under `{prefix}config/{key}` for tree configuration data
- **Content-addressed**: Nodes with the same content have the same hash, enabling deduplication

## Performance Considerations

### Caching

The S3 storage backend includes an LRU cache (default size: 1000 nodes) to reduce S3 API calls:

```rust
// Rust: Custom cache size
let storage = S3NodeStorage::with_cache_size(
    "my-bucket".to_string(),
    Some("prefix/".to_string()),
    5000  // Cache up to 5000 nodes
).await?;
```

### Batch Operations

Use batch operations when inserting multiple items to reduce the number of S3 API calls:

```python
# Instead of multiple inserts:
# for i in range(1000):
#     tree.insert(f"key{i}".encode(), f"value{i}".encode())

# Use batch insert:
items = [(f"key{i}".encode(), f"value{i}".encode()) for i in range(1000)]
tree.insert_batch(items)
```

### Cost Optimization

- **Request costs**: S3 charges per request. Use caching and batch operations to minimize requests
- **Storage costs**: Content-addressing means duplicate nodes are stored only once
- **Transfer costs**: Consider using S3 in the same region as your compute resources

## Testing

Run the S3 storage tests:

```bash
# Set up environment
export TEST_S3_BUCKET=my-test-bucket
export AWS_REGION=us-east-1

# Run tests
python -m pytest python/tests/test_s3_storage.py -v
```

Run the example:

```bash
export TEST_S3_BUCKET=my-test-bucket
export AWS_REGION=us-east-1
python python/examples/s3_storage_example.py
```

## Troubleshooting

### Authentication Errors

If you see authentication errors:

1. Verify AWS credentials are configured:
   ```bash
   aws sts get-caller-identity
   ```

2. Check credential files:
   ```bash
   cat ~/.aws/credentials
   cat ~/.aws/config
   ```

3. Verify environment variables:
   ```bash
   echo $AWS_ACCESS_KEY_ID
   echo $AWS_SECRET_ACCESS_KEY
   echo $AWS_REGION
   ```

### Permission Errors

Ensure your IAM user/role has the required S3 permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::my-bucket/prefix/*"
    }
  ]
}
```

### Bucket Not Found

Verify:
1. Bucket name is correct
2. Bucket exists in the specified region
3. You have access to the bucket

### Build Errors

If building with S3 support fails:

1. Ensure Rust toolchain is up to date: `rustup update`
2. Check that all dependencies are available
3. Try cleaning and rebuilding: `cargo clean && cargo build --features s3_storage`

## Limitations

- **Synchronous API**: While S3 operations are asynchronous internally, the public API is synchronous
- **No built-in tree reconstruction**: You need to track root hashes to reconstruct trees from S3
- **Single-writer**: Like other storage backends, concurrent writes from multiple processes are not supported
- **No garbage collection**: Deleted nodes remain in S3 until manually cleaned up

## Future Enhancements

Potential improvements for the S3 storage backend:

- [ ] Async API support
- [ ] Multi-part upload for large nodes
- [ ] Built-in garbage collection
- [ ] Versioning support via S3 versioning
- [ ] Support for S3-compatible services (MinIO, DigitalOcean Spaces, etc.)
- [ ] Optimistic locking for multi-writer scenarios
- [ ] Compression for stored nodes

## Related Documentation

- [ProllyTree Python Documentation](https://prollytree.readthedocs.io/)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [AWS SDK for Rust](https://github.com/awslabs/aws-sdk-rust)

## License

This feature is part of ProllyTree and is licensed under the Apache License 2.0.
