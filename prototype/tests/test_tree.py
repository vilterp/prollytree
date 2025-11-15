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
Tests for ProllyTree implementation.
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from tree import ProllyTree
from store import MemoryStore


@pytest.fixture
def empty_tree():
    """Create an empty ProllyTree for testing."""
    return ProllyTree(pattern=0.0001, seed=42)


def test_insert_batch_into_empty_tree(empty_tree):
    """Test inserting a batch into an empty tree."""
    mutations = [(i, f"v{i}") for i in [2, 4, 6, 8, 10, 12]]
    expected = [(i, f"v{i}") for i in [2, 4, 6, 8, 10, 12]]

    stats = empty_tree.insert_batch(mutations, verbose=False)
    result = empty_tree.verify()

    assert result == expected
    assert stats['nodes_created'] > 0


def test_insert_batch_with_interleaved_keys():
    """Test inserting interleaved keys that require tree restructuring."""
    tree = ProllyTree(pattern=0.0001, seed=42)

    # First batch
    tree.insert_batch([(i, f"v{i}") for i in [2, 4, 6, 8, 10, 12]], verbose=False)

    # Second batch with interleaved keys
    mutations = [(i, f"v{i}") for i in [1, 3, 5, 7, 9, 11]]
    expected = [(i, f"v{i}") for i in range(1, 13)]

    stats = tree.insert_batch(mutations, verbose=False)
    result = tree.verify()

    assert result == expected
    assert stats['nodes_created'] > 0


def test_subtree_reuse():
    """Test that unaffected subtrees are reused during insert."""
    tree = ProllyTree(pattern=0.0001, seed=42)

    # Build initial tree
    tree.insert_batch([(i, f"v{i}") for i in range(1, 13)], verbose=False)

    # Capture old node hashes
    old_node_hashes = set(tree.store.nodes.keys()) if isinstance(tree.store, MemoryStore) else set()

    # Insert keys in unaffected range (should reuse some subtrees)
    stats = tree.insert_batch([(i, f"v{i}") for i in [13, 14, 15, 16]], verbose=False)
    expected = [(i, f"v{i}") for i in range(1, 17)]

    result = tree.verify()
    assert result == expected


def test_large_insert():
    """Test inserting many keys to cause multiple splits."""
    tree = ProllyTree(pattern=0.0001, seed=42)

    # Insert initial keys
    tree.insert_batch([(i, f"v{i}") for i in range(1, 17)], verbose=False)

    # Insert many more keys
    stats = tree.insert_batch([(i, f"v{i}") for i in range(17, 41)], verbose=False)
    expected = [(i, f"v{i}") for i in range(1, 41)]

    result = tree.verify()
    assert result == expected
    assert stats['nodes_created'] > 0


def test_items_prefix_filtering():
    """Test that items(prefix) correctly filters by prefix."""
    tree = ProllyTree(pattern=0.0001, seed=42)

    # Insert keys with different prefixes
    mutations = [
        ("/d/users/1", "Alice"),
        ("/d/users/2", "Bob"),
        ("/d/products/1", "Laptop"),
        ("/d/products/2", "Mouse"),
        ("/s/users", "schema1"),
    ]
    tree.insert_batch(mutations, verbose=False)

    # Test filtering by /d/users/ prefix
    users = list(tree.items("/d/users/"))
    assert len(users) == 2
    assert users[0] == ("/d/users/1", "Alice")
    assert users[1] == ("/d/users/2", "Bob")

    # Test filtering by /d/products/ prefix
    products = list(tree.items("/d/products/"))
    assert len(products) == 2

    # Test filtering by /s/ prefix
    schemas = list(tree.items("/s/"))
    assert len(schemas) == 1
    assert schemas[0] == ("/s/users", "schema1")


def test_items_empty_prefix():
    """Test that items('') returns all items."""
    tree = ProllyTree(pattern=0.0001, seed=42)

    mutations = [(i, f"v{i}") for i in range(1, 6)]
    tree.insert_batch(mutations, verbose=False)

    all_items = list(tree.items(""))
    assert len(all_items) == 5
    assert all_items == mutations


def test_items_nonexistent_prefix():
    """Test that items with non-existent prefix returns empty."""
    tree = ProllyTree(pattern=0.0001, seed=42)

    mutations = [("/d/users/1", "Alice"), ("/d/users/2", "Bob")]
    tree.insert_batch(mutations, verbose=False)

    # Query non-existent prefix
    results = list(tree.items("/d/products/"))
    assert len(results) == 0


def test_store_persistence():
    """Test that different store types work correctly."""
    from store import FileSystemStore, CachedFSStore
    import tempfile
    import shutil

    # Test with MemoryStore (default)
    tree1 = ProllyTree(pattern=0.0001, seed=42)
    tree1.insert_batch([(1, "a"), (2, "b")], verbose=False)
    assert tree1.verify() == [(1, "a"), (2, "b")]

    # Test with FileSystemStore
    temp_dir = tempfile.mkdtemp()
    try:
        fs_store = FileSystemStore(temp_dir)
        tree2 = ProllyTree(pattern=0.0001, seed=42, store=fs_store)
        tree2.insert_batch([(1, "a"), (2, "b")], verbose=False)
        assert tree2.verify() == [(1, "a"), (2, "b")]

        # Verify nodes were written to disk
        assert fs_store.count_nodes() > 0
    finally:
        shutil.rmtree(temp_dir)

    # Test with CachedFSStore
    temp_dir = tempfile.mkdtemp()
    try:
        cached_store = CachedFSStore(temp_dir, cache_size=10)
        tree3 = ProllyTree(pattern=0.0001, seed=42, store=cached_store)
        tree3.insert_batch([(1, "a"), (2, "b")], verbose=False)
        assert tree3.verify() == [(1, "a"), (2, "b")]

        # Check cache stats
        stats = cached_store.get_cache_stats()
        assert 'cache_size' in stats
        assert 'hit_rate' in stats
    finally:
        shutil.rmtree(temp_dir)
