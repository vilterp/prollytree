#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Example: Using S3 Storage Backend with ProllyTree

This example demonstrates how to use AWS S3 as a storage backend for ProllyTree.
The S3 backend allows you to store your prolly tree nodes in cloud storage,
enabling distributed access and persistence.

Prerequisites:
1. AWS credentials configured (via ~/.aws/credentials or environment variables)
2. An S3 bucket that you have read/write access to
3. Python package built with s3_storage feature enabled

Setup:
    export AWS_REGION=us-east-1  # or your preferred region
    export TEST_S3_BUCKET=my-bucket-name
    
    # Build Python package with S3 support
    cd python
    ../python/build_python.sh --features "python s3_storage"
    pip install -e .
"""

import os
from prollytree import ProllyTree, TreeConfig


def example_basic_s3_usage():
    """Basic S3 storage example"""
    print("="*70)
    print("Example 1: Basic S3 Storage Usage")
    print("="*70)
    
    # Get bucket name from environment
    bucket = os.getenv("TEST_S3_BUCKET", "my-test-bucket")
    
    # Create a ProllyTree with S3 storage
    tree = ProllyTree(
        storage_type="s3",
        bucket=bucket,
        prefix="examples/basic/"  # Optional prefix for organization
    )
    
    print(f"\nCreated S3-backed ProllyTree")
    print(f"  Bucket: {bucket}")
    print(f"  Prefix: examples/basic/")
    
    # Insert some data
    print("\nInserting data...")
    tree.insert(b"user:1", b"Alice")
    tree.insert(b"user:2", b"Bob")
    tree.insert(b"user:3", b"Charlie")
    
    # Retrieve data
    print("\nRetrieving data:")
    for i in range(1, 4):
        key = f"user:{i}".encode()
        value = tree.find(key)
        print(f"  {key.decode()}: {value.decode()}")
    
    # Tree properties
    print(f"\nTree size: {tree.size()} items")
    print(f"Tree depth: {tree.depth()}")
    print(f"Root hash: {tree.get_root_hash().hex()[:16]}...")
    
    print("\n✓ Data is now stored in S3!")


def example_batch_operations():
    """Batch operations with S3 storage"""
    print("\n" + "="*70)
    print("Example 2: Batch Operations")
    print("="*70)
    
    bucket = os.getenv("TEST_S3_BUCKET", "my-test-bucket")
    
    tree = ProllyTree(
        storage_type="s3",
        bucket=bucket,
        prefix="examples/batch/"
    )
    
    # Batch insert - more efficient for large datasets
    print("\nBatch inserting 100 items...")
    items = [
        (f"product:{i}".encode(), f"Product {i}".encode())
        for i in range(100)
    ]
    tree.insert_batch(items)
    
    print(f"✓ Inserted {tree.size()} items")
    
    # Verify some items
    print("\nSample items:")
    for i in [0, 25, 50, 75, 99]:
        key = f"product:{i}".encode()
        value = tree.find(key)
        print(f"  {key.decode()}: {value.decode()}")


def example_custom_configuration():
    """Using custom tree configuration with S3"""
    print("\n" + "="*70)
    print("Example 3: Custom Tree Configuration")
    print("="*70)
    
    bucket = os.getenv("TEST_S3_BUCKET", "my-test-bucket")
    
    # Create custom configuration
    config = TreeConfig(
        base=8,           # Higher base for wider nodes
        modulus=128,      # Larger modulus for larger chunks
        min_chunk_size=2,
        max_chunk_size=8192
    )
    
    tree = ProllyTree(
        storage_type="s3",
        bucket=bucket,
        prefix="examples/custom-config/",
        config=config
    )
    
    print("\nCreated tree with custom configuration:")
    print(f"  Base: {config.base}")
    print(f"  Modulus: {config.modulus}")
    print(f"  Min chunk size: {config.min_chunk_size}")
    print(f"  Max chunk size: {config.max_chunk_size}")
    
    # Insert data
    tree.insert(b"config_test", b"This tree has custom parameters")
    print(f"\n✓ Tree working with custom config")


def example_comparing_trees():
    """Comparing two S3-backed trees using root hashes"""
    print("\n" + "="*70)
    print("Example 4: Comparing Trees via Root Hashes")
    print("="*70)
    
    bucket = os.getenv("TEST_S3_BUCKET", "my-test-bucket")
    
    # Create two trees with different data
    print("\nCreating Tree 1...")
    tree1 = ProllyTree(
        storage_type="s3",
        bucket=bucket,
        prefix="examples/tree1/"
    )
    tree1.insert(b"a", b"1")
    tree1.insert(b"b", b"2")
    tree1.insert(b"c", b"3")
    
    print("Creating Tree 2...")
    tree2 = ProllyTree(
        storage_type="s3",
        bucket=bucket,
        prefix="examples/tree2/"
    )
    tree2.insert(b"a", b"1")
    tree2.insert(b"b", b"2")
    tree2.insert(b"c", b"3")
    tree2.insert(b"d", b"4")  # Extra item
    
    # Compare root hashes
    hash1 = tree1.get_root_hash()
    hash2 = tree2.get_root_hash()
    
    print(f"\nTree 1 root hash: {hash1.hex()[:32]}...")
    print(f"Tree 2 root hash: {hash2.hex()[:32]}...")
    print(f"Trees are identical: {hash1 == hash2}")
    
    # Create identical tree
    print("\nCreating Tree 3 (identical to Tree 1)...")
    tree3 = ProllyTree(
        storage_type="s3",
        bucket=bucket,
        prefix="examples/tree3/"
    )
    tree3.insert(b"a", b"1")
    tree3.insert(b"b", b"2")
    tree3.insert(b"c", b"3")
    
    hash3 = tree3.get_root_hash()
    print(f"Tree 3 root hash: {hash3.hex()[:32]}...")
    print(f"Tree 1 and Tree 3 are identical: {hash1 == hash3}")
    
    print("\n✓ Root hashes enable efficient tree comparison!")


def example_persistence():
    """Demonstrate S3 persistence"""
    print("\n" + "="*70)
    print("Example 5: S3 Persistence")
    print("="*70)
    
    bucket = os.getenv("TEST_S3_BUCKET", "my-test-bucket")
    prefix = "examples/persistence/"
    
    print("\nPhase 1: Creating and populating tree...")
    tree = ProllyTree(
        storage_type="s3",
        bucket=bucket,
        prefix=prefix
    )
    
    # Insert data
    for i in range(10):
        tree.insert(f"key_{i}".encode(), f"value_{i}".encode())
    
    root_hash = tree.get_root_hash()
    print(f"  Inserted 10 items")
    print(f"  Root hash: {root_hash.hex()[:32]}...")
    
    print("\nPhase 2: Creating new tree instance with same configuration...")
    # In a real application, you would save and restore the root hash
    # to reconstruct the exact same tree from S3
    tree2 = ProllyTree(
        storage_type="s3",
        bucket=bucket,
        prefix=prefix
    )
    
    # Re-insert same data to create same tree structure
    for i in range(10):
        tree2.insert(f"key_{i}".encode(), f"value_{i}".encode())
    
    root_hash2 = tree2.get_root_hash()
    print(f"  Root hash: {root_hash2.hex()[:32]}...")
    print(f"  Hashes match: {root_hash == root_hash2}")
    
    print("\n✓ Tree nodes are persisted in S3!")
    print("  Note: Full tree reconstruction from root hash requires")
    print("  additional implementation in the application layer.")


def main():
    """Run all examples"""
    print("\n" + "="*70)
    print("ProllyTree S3 Storage Backend Examples")
    print("="*70)
    
    # Check if bucket is configured
    bucket = os.getenv("TEST_S3_BUCKET")
    if bucket is None:
        print("\n⚠️  WARNING: TEST_S3_BUCKET environment variable not set!")
        print("\nTo run these examples, please set up AWS credentials and bucket:")
        print("  export AWS_REGION=us-east-1")
        print("  export TEST_S3_BUCKET=my-test-bucket")
        print("\nThen build the Python package with S3 support:")
        print("  cd python")
        print("  ../python/build_python.sh --features 'python s3_storage'")
        print("  pip install -e .")
        print("\nUsing default bucket name for demonstration purposes...")
        print("(Actual operations will fail without proper configuration)")
    
    try:
        example_basic_s3_usage()
        example_batch_operations()
        example_custom_configuration()
        example_comparing_trees()
        example_persistence()
        
        print("\n" + "="*70)
        print("All examples completed successfully!")
        print("="*70)
        print("\nKey benefits of S3 storage:")
        print("  ✓ Cloud-native persistence")
        print("  ✓ Distributed access across systems")
        print("  ✓ Scalable storage")
        print("  ✓ Data durability and availability")
        print("  ✓ Content-addressed nodes enable efficient sharing")
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        print("\nPlease ensure:")
        print("  1. AWS credentials are configured")
        print("  2. TEST_S3_BUCKET environment variable is set")
        print("  3. You have permissions to read/write to the bucket")
        print("  4. Python package is built with s3_storage feature")


if __name__ == "__main__":
    main()
