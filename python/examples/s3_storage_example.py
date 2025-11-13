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
Example: Using S3 Storage Backend for ProllyTree

This example demonstrates how to use the S3 storage backend with ProllyTree,
including diff operations between two S3-backed trees.

Prerequisites:
1. Build ProllyTree with S3 support:
   cd python && ./build_python.sh --with-s3 --install

2. Set up AWS credentials (or use LocalStack):
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_REGION=us-east-1

3. Set S3 bucket name:
   export S3_BUCKET=your-bucket-name

For LocalStack testing:
   docker run -d -p 4566:4566 localstack/localstack
   export S3_ENDPOINT_URL=http://localhost:4566
   export S3_BUCKET=test-bucket
   export AWS_ACCESS_KEY_ID=test
   export AWS_SECRET_ACCESS_KEY=test
   export AWS_REGION=us-east-1
"""

import os
import sys


def main():
    try:
        from prollytree import ProllyTree, S3Config
    except ImportError:
        print("Error: ProllyTree with S3 support is not installed.")
        print("Build and install with: cd python && ./build_python.sh --with-s3 --install")
        sys.exit(1)

    # Get configuration from environment
    bucket = os.environ.get("S3_BUCKET")
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    region = os.environ.get("AWS_REGION", "us-east-1")

    if not bucket:
        print("Error: S3_BUCKET environment variable is not set")
        print("Set it with: export S3_BUCKET=your-bucket-name")
        sys.exit(1)

    print("=" * 60)
    print("S3 Storage Backend Example")
    print("=" * 60)
    print(f"Bucket: {bucket}")
    print(f"Region: {region}")
    if endpoint_url:
        print(f"Endpoint: {endpoint_url}")
    print()

    # Example 1: Basic S3 storage operations
    print("Example 1: Basic S3 Storage Operations")
    print("-" * 60)

    s3_config = S3Config(
        bucket=bucket,
        prefix="prollytree/example1/",
        region=region,
        endpoint_url=endpoint_url,
        cache_size=1000,
    )

    tree = ProllyTree(storage_type="s3", s3_config=s3_config)

    # Insert some data
    print("Inserting data...")
    tree.insert(b"name", b"Alice")
    tree.insert(b"age", b"30")
    tree.insert(b"city", b"San Francisco")

    # Retrieve data
    print("Retrieving data...")
    print(f"  name: {tree.get(b'name').decode()}")
    print(f"  age: {tree.get(b'age').decode()}")
    print(f"  city: {tree.get(b'city').decode()}")
    print()

    # Example 2: Diff between two S3-backed trees
    print("Example 2: Diff Between Two S3-Backed Trees")
    print("-" * 60)

    # Create first tree (e.g., production data)
    s3_config_prod = S3Config(
        bucket=bucket,
        prefix="prollytree/production/",
        region=region,
        endpoint_url=endpoint_url,
        cache_size=500,
    )
    tree_prod = ProllyTree(storage_type="s3", s3_config=s3_config_prod)

    print("Production tree data:")
    tree_prod.insert(b"user:1:name", b"Alice")
    tree_prod.insert(b"user:1:email", b"alice@example.com")
    tree_prod.insert(b"user:2:name", b"Bob")
    tree_prod.insert(b"user:2:email", b"bob@example.com")
    tree_prod.insert(b"config:version", b"1.0.0")
    print("  - 5 entries inserted")

    # Create second tree (e.g., staging data with changes)
    s3_config_staging = S3Config(
        bucket=bucket,
        prefix="prollytree/staging/",
        region=region,
        endpoint_url=endpoint_url,
        cache_size=500,
    )
    tree_staging = ProllyTree(storage_type="s3", s3_config=s3_config_staging)

    print("\nStaging tree data:")
    tree_staging.insert(b"user:1:name", b"Alice Smith")  # Modified
    tree_staging.insert(b"user:1:email", b"alice@example.com")  # Same
    tree_staging.insert(b"user:2:name", b"Bob")  # Same
    tree_staging.insert(b"user:2:email", b"bob.jones@example.com")  # Modified
    tree_staging.insert(b"user:3:name", b"Charlie")  # New
    tree_staging.insert(b"user:3:email", b"charlie@example.com")  # New
    tree_staging.insert(b"config:version", b"1.1.0")  # Modified
    print("  - 7 entries inserted")

    # Compute diff
    print("\nComputing diff between production and staging...")
    diff = tree_prod.diff(tree_staging)

    print(f"\nFound {len(diff)} differences:")
    for i, change in enumerate(diff, 1):
        print(f"\n{i}. {change}")

    print()

    # Example 3: Persistence across instances
    print("Example 3: Data Persistence Across Tree Instances")
    print("-" * 60)

    prefix = "prollytree/persistence/"

    # Create tree and insert data
    s3_config_1 = S3Config(
        bucket=bucket,
        prefix=prefix,
        region=region,
        endpoint_url=endpoint_url,
    )
    tree_1 = ProllyTree(storage_type="s3", s3_config=s3_config_1)
    tree_1.insert(b"persistent_key", b"persistent_value")
    root_hash = tree_1.root_hash()
    print(f"Stored data with root hash: {root_hash}")

    # Create new tree instance and load from hash
    s3_config_2 = S3Config(
        bucket=bucket,
        prefix=prefix,
        region=region,
        endpoint_url=endpoint_url,
    )
    tree_2 = ProllyTree(storage_type="s3", s3_config=s3_config_2)
    tree_2.load_from_hash(root_hash)
    value = tree_2.get(b"persistent_key")
    print(f"Retrieved value from new instance: {value.decode()}")
    print()

    print("=" * 60)
    print("All examples completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
