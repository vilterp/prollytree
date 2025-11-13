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
Test S3 storage backend for ProllyTree.

NOTE: These tests require AWS credentials and a real S3 bucket.
Set the following environment variables:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION (optional, defaults to us-east-1)
- S3_TEST_BUCKET (required)
- S3_ENDPOINT_URL (optional, for LocalStack or MinIO)

To run these tests:
    pytest python/tests/test_s3_storage.py -v

To run with LocalStack:
    docker run -d -p 4566:4566 localstack/localstack
    export S3_ENDPOINT_URL=http://localhost:4566
    export S3_TEST_BUCKET=test-bucket
    export AWS_ACCESS_KEY_ID=test
    export AWS_SECRET_ACCESS_KEY=test
    pytest python/tests/test_s3_storage.py -v
"""

import os
import pytest


# Check if S3 storage feature is available
try:
    from prollytree import S3Config, ProllyTree
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False
    pytestmark = pytest.mark.skip(reason="S3 storage feature not available")


# Check if required environment variables are set
S3_TEST_BUCKET = os.environ.get("S3_TEST_BUCKET")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

if not S3_TEST_BUCKET and S3_AVAILABLE:
    pytestmark = pytest.mark.skip(
        reason="S3_TEST_BUCKET environment variable not set. "
        "Set S3_TEST_BUCKET to run these tests."
    )


@pytest.mark.skipif(not S3_AVAILABLE, reason="S3 storage feature not available")
class TestS3Storage:
    """Tests for S3 storage backend."""

    def test_s3_config_creation(self):
        """Test S3Config creation with various parameters."""
        # Basic config
        config = S3Config(bucket="test-bucket")
        assert config is not None

        # Config with all parameters
        config = S3Config(
            bucket="test-bucket",
            prefix="prollytree/test/",
            region="us-west-2",
            endpoint_url="http://localhost:4566",
            cache_size=2000,
        )
        assert config is not None

    @pytest.mark.skipif(not S3_TEST_BUCKET, reason="S3_TEST_BUCKET not set")
    def test_s3_tree_basic_operations(self):
        """Test basic ProllyTree operations with S3 storage."""
        # Create S3 config
        s3_config = S3Config(
            bucket=S3_TEST_BUCKET,
            prefix="test/basic/",
            region=AWS_REGION,
            endpoint_url=S3_ENDPOINT_URL,
        )

        # Create tree with S3 storage
        tree = ProllyTree(storage_type="s3", s3_config=s3_config)

        # Insert some data
        tree.insert(b"key1", b"value1")
        tree.insert(b"key2", b"value2")
        tree.insert(b"key3", b"value3")

        # Retrieve data
        assert tree.get(b"key1") == b"value1"
        assert tree.get(b"key2") == b"value2"
        assert tree.get(b"key3") == b"value3"

        # Check non-existent key
        assert tree.get(b"nonexistent") is None

    @pytest.mark.skipif(not S3_TEST_BUCKET, reason="S3_TEST_BUCKET not set")
    def test_s3_tree_diff_operations(self):
        """Test diff operations between two S3-backed ProllyTree instances."""
        # Create first tree
        s3_config1 = S3Config(
            bucket=S3_TEST_BUCKET,
            prefix="test/diff/tree1/",
            region=AWS_REGION,
            endpoint_url=S3_ENDPOINT_URL,
            cache_size=500,
        )
        tree1 = ProllyTree(storage_type="s3", s3_config=s3_config1)

        # Create second tree
        s3_config2 = S3Config(
            bucket=S3_TEST_BUCKET,
            prefix="test/diff/tree2/",
            region=AWS_REGION,
            endpoint_url=S3_ENDPOINT_URL,
            cache_size=500,
        )
        tree2 = ProllyTree(storage_type="s3", s3_config=s3_config2)

        # Insert data into both trees
        tree1.insert(b"key1", b"value1")
        tree1.insert(b"key2", b"value2")
        tree1.insert(b"shared", b"same_value")

        tree2.insert(b"key2", b"value2_modified")
        tree2.insert(b"key3", b"value3")
        tree2.insert(b"shared", b"same_value")

        # Get diff between trees
        diff = tree1.diff(tree2)

        # Verify diff results
        assert len(diff) > 0
        print(f"\nDiff results: {diff}")

        # Verify specific differences
        # - key1 exists only in tree1
        # - key2 has different values
        # - key3 exists only in tree2
        # - shared has the same value in both

    @pytest.mark.skipif(not S3_TEST_BUCKET, reason="S3_TEST_BUCKET not set")
    def test_s3_tree_with_custom_cache(self):
        """Test S3 storage with custom cache size."""
        s3_config = S3Config(
            bucket=S3_TEST_BUCKET,
            prefix="test/cache/",
            region=AWS_REGION,
            endpoint_url=S3_ENDPOINT_URL,
            cache_size=100,  # Small cache for testing
        )

        tree = ProllyTree(storage_type="s3", s3_config=s3_config)

        # Insert many items to test cache behavior
        for i in range(200):
            key = f"key{i}".encode()
            value = f"value{i}".encode()
            tree.insert(key, value)

        # Verify all items are retrievable
        for i in range(200):
            key = f"key{i}".encode()
            value = f"value{i}".encode()
            assert tree.get(key) == value

    @pytest.mark.skipif(not S3_TEST_BUCKET, reason="S3_TEST_BUCKET not set")
    def test_s3_tree_persistence(self):
        """Test that data persists in S3 across tree instances."""
        prefix = "test/persistence/"

        # Create first tree and insert data
        s3_config1 = S3Config(
            bucket=S3_TEST_BUCKET,
            prefix=prefix,
            region=AWS_REGION,
            endpoint_url=S3_ENDPOINT_URL,
        )
        tree1 = ProllyTree(storage_type="s3", s3_config=s3_config1)
        tree1.insert(b"persistent_key", b"persistent_value")

        # Get the root hash
        root_hash1 = tree1.root_hash()

        # Create second tree with same config
        s3_config2 = S3Config(
            bucket=S3_TEST_BUCKET,
            prefix=prefix,
            region=AWS_REGION,
            endpoint_url=S3_ENDPOINT_URL,
        )
        tree2 = ProllyTree(storage_type="s3", s3_config=s3_config2)

        # Load from same root hash
        tree2.load_from_hash(root_hash1)

        # Verify data is accessible
        assert tree2.get(b"persistent_key") == b"persistent_value"


if __name__ == "__main__":
    # Run tests if this file is executed directly
    pytest.main([__file__, "-v"])
