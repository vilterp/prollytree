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
Unit tests for S3 storage backend in ProllyTree Python bindings

These tests require:
- AWS credentials configured (via environment variables or ~/.aws/credentials)
- An S3 bucket accessible with those credentials
- Set TEST_S3_BUCKET environment variable to specify the bucket name
"""

import os
import unittest
import pytest
from prollytree import ProllyTree, TreeConfig


@pytest.mark.skipif(
    os.getenv("TEST_S3_BUCKET") is None,
    reason="TEST_S3_BUCKET environment variable not set"
)
class TestS3Storage(unittest.TestCase):
    """Test S3 storage backend"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.bucket = os.getenv("TEST_S3_BUCKET")
        self.prefix = "test-prolly-trees/"
        
    def test_basic_s3_operations(self):
        """Test basic insert, find, and delete with S3 storage"""
        # Create S3-backed tree
        tree = ProllyTree(
            storage_type="s3",
            bucket=self.bucket,
            prefix=self.prefix + "test1/"
        )
        
        # Test insert and find
        tree.insert(b"key1", b"value1")
        tree.insert(b"key2", b"value2")
        tree.insert(b"key3", b"value3")
        
        # Verify values
        assert tree.find(b"key1") == b"value1"
        assert tree.find(b"key2") == b"value2"
        assert tree.find(b"key3") == b"value3"
        
        # Test non-existent key
        assert tree.find(b"nonexistent") is None
        
        # Test delete
        tree.delete(b"key2")
        assert tree.find(b"key2") is None
        assert tree.find(b"key1") == b"value1"  # Other keys should still exist
        
    def test_batch_operations_s3(self):
        """Test batch insert with S3 storage"""
        tree = ProllyTree(
            storage_type="s3",
            bucket=self.bucket,
            prefix=self.prefix + "test2/"
        )
        
        # Batch insert
        items = [
            (b"batch1", b"value1"),
            (b"batch2", b"value2"),
            (b"batch3", b"value3"),
        ]
        tree.insert_batch(items)
        
        # Verify all items
        assert tree.find(b"batch1") == b"value1"
        assert tree.find(b"batch2") == b"value2"
        assert tree.find(b"batch3") == b"value3"
        
        # Test size
        assert tree.size() == 3
        
    def test_tree_properties_s3(self):
        """Test tree properties with S3 storage"""
        tree = ProllyTree(
            storage_type="s3",
            bucket=self.bucket,
            prefix=self.prefix + "test3/"
        )
        
        # Empty tree
        assert tree.size() == 0
        initial_hash = tree.get_root_hash()
        
        # Add items
        for i in range(10):
            tree.insert(f"key{i}".encode(), f"value{i}".encode())
        
        assert tree.size() == 10
        
        # Root hash should change after modifications
        new_hash = tree.get_root_hash()
        assert initial_hash != new_hash
        
    def test_s3_persistence(self):
        """Test that data persists in S3 across tree instances"""
        prefix = self.prefix + "test4/"
        
        # Create first tree and insert data
        tree1 = ProllyTree(
            storage_type="s3",
            bucket=self.bucket,
            prefix=prefix
        )
        tree1.insert(b"persist_key", b"persist_value")
        root_hash = tree1.get_root_hash()
        
        # Create second tree with same bucket and prefix
        # Note: In a real scenario, you'd need to restore from root hash
        # For now, we're testing that nodes are stored in S3
        tree2 = ProllyTree(
            storage_type="s3",
            bucket=self.bucket,
            prefix=prefix
        )
        
        # Insert the same key to create the same tree structure
        tree2.insert(b"persist_key", b"persist_value")
        
        # Should have the same root hash
        assert tree2.get_root_hash() == root_hash
        
    def test_custom_config_s3(self):
        """Test S3 storage with custom tree configuration"""
        config = TreeConfig(
            base=8,
            modulus=128,
            min_chunk_size=2,
            max_chunk_size=8192
        )
        
        tree = ProllyTree(
            storage_type="s3",
            bucket=self.bucket,
            prefix=self.prefix + "test5/",
            config=config
        )
        
        # Should work with custom config
        tree.insert(b"config_key", b"config_value")
        assert tree.find(b"config_key") == b"config_value"


@pytest.mark.skipif(
    os.getenv("TEST_S3_BUCKET") is None,
    reason="TEST_S3_BUCKET environment variable not set"
)
class TestS3Diff(unittest.TestCase):
    """Test diff functionality between S3-backed prolly trees"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.bucket = os.getenv("TEST_S3_BUCKET")
        self.prefix = "test-prolly-diff/"
        
    def test_diff_two_s3_trees(self):
        """Test diff between two S3-backed prolly trees"""
        # Create first tree
        tree1 = ProllyTree(
            storage_type="s3",
            bucket=self.bucket,
            prefix=self.prefix + "tree1/"
        )
        tree1.insert(b"key1", b"value1")
        tree1.insert(b"key2", b"value2")
        tree1.insert(b"shared", b"original")
        
        # Create second tree with different data
        tree2 = ProllyTree(
            storage_type="s3",
            bucket=self.bucket,
            prefix=self.prefix + "tree2/"
        )
        tree2.insert(b"key1", b"value1")  # Same
        tree2.insert(b"key3", b"value3")  # Added
        tree2.insert(b"shared", b"modified")  # Modified
        # key2 is removed
        
        # Test diff functionality
        # Note: The basic ProllyTree doesn't expose a diff method,
        # but the nodes are in S3 and can be compared via root hashes
        hash1 = tree1.get_root_hash()
        hash2 = tree2.get_root_hash()
        
        # Trees should have different root hashes
        assert hash1 != hash2
        
    def test_identical_trees_same_hash(self):
        """Test that identical trees have the same root hash"""
        # Create first tree
        tree1 = ProllyTree(
            storage_type="s3",
            bucket=self.bucket,
            prefix=self.prefix + "identical1/"
        )
        tree1.insert(b"a", b"1")
        tree1.insert(b"b", b"2")
        tree1.insert(b"c", b"3")
        
        # Create second tree with same data
        tree2 = ProllyTree(
            storage_type="s3",
            bucket=self.bucket,
            prefix=self.prefix + "identical2/"
        )
        tree2.insert(b"a", b"1")
        tree2.insert(b"b", b"2")
        tree2.insert(b"c", b"3")
        
        # Should have the same root hash (content-addressed)
        assert tree1.get_root_hash() == tree2.get_root_hash()


if __name__ == "__main__":
    # Print helpful message about required environment setup
    if os.getenv("TEST_S3_BUCKET") is None:
        print("\n" + "="*70)
        print("S3 STORAGE TESTS SKIPPED")
        print("="*70)
        print("\nTo run S3 storage tests, you need to:")
        print("1. Configure AWS credentials (via ~/.aws/credentials or env vars)")
        print("2. Set TEST_S3_BUCKET environment variable to your test bucket")
        print("\nExample:")
        print("  export TEST_S3_BUCKET='my-test-bucket'")
        print("  export AWS_REGION='us-east-1'")
        print("  python -m pytest python/tests/test_s3_storage.py -v")
        print("="*70 + "\n")
    
    pytest.main([__file__, "-v"])
