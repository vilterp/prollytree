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
Tests for diff algorithm.
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from tree import ProllyTree
from store import MemoryStore
from diff import diff, Differ, Added, Deleted, Modified


@pytest.fixture
def store():
    """Create a shared MemoryStore for testing."""
    return MemoryStore()


def test_diff_identical_trees(store):
    """Test that identical trees produce no diff events."""
    tree = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree.insert_batch([(i, f"v{i}") for i in range(1, 6)], verbose=False)
    root_hash = tree._hash_node(tree.root)

    # Diff tree with itself
    events = list(diff(store, root_hash, root_hash))

    assert len(events) == 0


def test_diff_additions_only(store):
    """Test diff when only additions are present."""
    # Create tree1 with initial data
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree1.insert_batch([(1, "a"), (2, "b")], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with additional data
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([(1, "a"), (2, "b"), (3, "c"), (4, "d")], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Diff tree1 -> tree2
    events = list(diff(store, hash1, hash2))

    assert len(events) == 2
    assert events[0] == Added(3, "c")
    assert events[1] == Added(4, "d")


def test_diff_deletions_only(store):
    """Test diff when only deletions are present."""
    # Create tree1 with data
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree1.insert_batch([(1, "a"), (2, "b"), (3, "c"), (4, "d")], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with subset of data
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([(1, "a"), (2, "b")], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Diff tree1 -> tree2
    events = list(diff(store, hash1, hash2))

    assert len(events) == 2
    assert events[0] == Deleted(3, "c")
    assert events[1] == Deleted(4, "d")


def test_diff_modifications_only(store):
    """Test diff when only modifications are present."""
    # Create tree1 with initial data
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree1.insert_batch([(1, "a"), (2, "b"), (3, "c")], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with modified values
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([(1, "a"), (2, "B"), (3, "C")], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Diff tree1 -> tree2
    events = list(diff(store, hash1, hash2))

    assert len(events) == 2
    assert events[0] == Modified(2, "b", "B")
    assert events[1] == Modified(3, "c", "C")


def test_diff_mixed_changes(store):
    """Test diff with additions, deletions, and modifications."""
    # Create tree1
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree1.insert_batch([(1, "a"), (2, "b"), (3, "c"), (5, "e")], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with mixed changes
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([(1, "A"), (2, "b"), (4, "d"), (5, "e")], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Diff tree1 -> tree2
    events = list(diff(store, hash1, hash2))

    # Expected: Modified(1, "a" -> "A"), Deleted(3), Added(4, "d")
    assert len(events) == 3
    assert events[0] == Modified(1, "a", "A")
    assert events[1] == Deleted(3, "c")
    assert events[2] == Added(4, "d")


def test_diff_large_trees(store):
    """Test diff on larger trees to verify subtree skipping."""
    # Create tree1 with 100 entries
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    entries1 = [(i, f"v{i}") for i in range(1, 101)]
    tree1.insert_batch(entries1, verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with modifications in middle range
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    entries2 = [(i, f"v{i}") for i in range(1, 101)]
    # Modify entries 40-60
    for i in range(40, 61):
        entries2[i - 1] = (i, f"V{i}")  # Uppercase V
    tree2.insert_batch(entries2, verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Diff should only show modifications in the middle range
    events = list(diff(store, hash1, hash2))

    assert len(events) == 21  # Entries 40-60 (inclusive)
    for i, event in enumerate(events):
        key = 40 + i
        assert event == Modified(key, f"v{key}", f"V{key}")


def test_diff_empty_to_populated(store):
    """Test diff from empty tree to populated tree."""
    # Create empty tree
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    hash1 = tree1._hash_node(tree1.root)

    # Create populated tree
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([(1, "a"), (2, "b"), (3, "c")], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Diff empty -> populated
    events = list(diff(store, hash1, hash2))

    assert len(events) == 3
    assert events[0] == Added(1, "a")
    assert events[1] == Added(2, "b")
    assert events[2] == Added(3, "c")


def test_diff_populated_to_empty(store):
    """Test diff from populated tree to empty tree."""
    # Create populated tree
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree1.insert_batch([(1, "a"), (2, "b"), (3, "c")], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create empty tree
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    hash2 = tree2._hash_node(tree2.root)

    # Diff populated -> empty
    events = list(diff(store, hash1, hash2))

    assert len(events) == 3
    assert events[0] == Deleted(1, "a")
    assert events[1] == Deleted(2, "b")
    assert events[2] == Deleted(3, "c")


def test_diff_with_string_keys(store):
    """Test diff with string keys (like database tables)."""
    # Create tree1
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree1.insert_batch([
        ("/d/users/1", "Alice"),
        ("/d/users/2", "Bob"),
        ("/d/products/1", "Laptop"),
    ], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with changes
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([
        ("/d/users/1", "Alice Smith"),  # Modified
        ("/d/users/3", "Charlie"),       # Added
        ("/d/products/1", "Laptop"),     # Unchanged
    ], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Diff tree1 -> tree2
    events = list(diff(store, hash1, hash2))

    assert len(events) == 3
    assert events[0] == Modified("/d/users/1", "Alice", "Alice Smith")
    assert events[1] == Deleted("/d/users/2", "Bob")
    assert events[2] == Added("/d/users/3", "Charlie")


def test_diff_subtree_skipping(store):
    """Test that identical subtrees are skipped (performance optimization)."""
    # Create a large tree
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    # Insert 1-50 in one batch
    tree1.insert_batch([(i, f"v{i}") for i in range(1, 51)], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with same data
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([(i, f"v{i}") for i in range(1, 51)], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Hashes should be identical
    assert hash1 == hash2

    # Diff should produce no events
    events = list(diff(store, hash1, hash2))
    assert len(events) == 0


def test_diff_partial_overlap(store):
    """Test diff with partial overlap between trees."""
    # Create tree1 with keys 1-10
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree1.insert_batch([(i, f"v{i}") for i in range(1, 11)], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with keys 6-15
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([(i, f"v{i}") for i in range(6, 16)], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Diff tree1 -> tree2
    events = list(diff(store, hash1, hash2))

    # Expected: Deleted(1-5), Added(11-15)
    deleted = [e for e in events if isinstance(e, Deleted)]
    added = [e for e in events if isinstance(e, Added)]

    assert len(deleted) == 5
    assert len(added) == 5
    assert all(e.key in range(1, 6) for e in deleted)
    assert all(e.key in range(11, 16) for e in added)


def test_diff_events_are_ordered(store):
    """Test that diff events are yielded in key order."""
    # Create tree1
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree1.insert_batch([(10, "j"), (20, "t"), (30, "th")], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with scattered changes
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([(5, "e"), (10, "J"), (25, "tw"), (30, "th")], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Diff should be in key order
    events = list(diff(store, hash1, hash2))

    # Extract keys in order
    keys = []
    for event in events:
        if isinstance(event, (Added, Deleted)):
            keys.append(event.key)
        elif isinstance(event, Modified):
            keys.append(event.key)

    # Keys should be sorted
    assert keys == sorted(keys)


def test_diff_event_repr():
    """Test string representation of diff events."""
    added = Added(1, "a")
    deleted = Deleted(2, "b")
    modified = Modified(3, "old", "new")

    assert repr(added) == "Added(1, 'a')"
    assert repr(deleted) == "Deleted(2, 'b')"
    assert repr(modified) == "Modified(3, 'old' -> 'new')"


def test_differ_statistics(store):
    """Test that Differ tracks subtree skip statistics."""
    # Create tree1 with 100 entries
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    entries1 = [(i, f"v{i}") for i in range(1, 101)]
    tree1.insert_batch(entries1, verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 - identical to tree1
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    entries2 = [(i, f"v{i}") for i in range(1, 101)]
    tree2.insert_batch(entries2, verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Use Differ to track stats
    differ = Differ(store)
    events = list(differ.diff(hash1, hash2))

    # Identical trees should have no events
    assert len(events) == 0

    # Should have skipped the root (since hashes are identical)
    stats = differ.get_stats()
    assert stats.subtrees_skipped == 1
    assert stats.nodes_compared == 0


def test_diff_identical_values_no_change(store):
    """Test that identical values don't produce diff events (regression test)."""
    schema_json = '{"columns":["i","j","ckt"],"types":["INTEGER","INTEGER","TEXT"],"primary_key":["i","j","ckt"]}'

    # Create tree1 with schema
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree1.insert_batch([("/s/lines", schema_json)], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with identical schema
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([("/s/lines", schema_json)], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Hashes should be identical
    assert hash1 == hash2, "Trees with identical data should have identical hashes"

    # Diff should produce no events
    events = list(diff(store, hash1, hash2))
    assert len(events) == 0, f"Expected no diff events for identical values, got {events}"


def test_diff_different_trees_same_value(store):
    """Test that when different trees have the same value for a key, no diff is shown."""
    schema_json = '{"columns":["i","j","ckt"],"types":["INTEGER","INTEGER","TEXT"],"primary_key":["i","j","ckt"]}'

    # Create tree1 with schema + other data
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree1.insert_batch([
        ("/s/lines", schema_json),
        ("/d/table1/1", "data1"),
        ("/d/table1/2", "data2"),
    ], verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with same schema but different other data
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    tree2.insert_batch([
        ("/s/lines", schema_json),  # Same value!
        ("/d/table1/1", "data1"),
        ("/d/table1/3", "data3"),  # Different row
    ], verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Trees should have different hashes (different data)
    assert hash1 != hash2, "Trees with different data should have different hashes"

    # Diff with prefix filter for /s/lines only
    events = list(diff(store, hash1, hash2, prefix="/s/lines"))

    # Should have NO events for /s/lines since the value is identical
    assert len(events) == 0, f"Expected no diff events for identical value at /s/lines, got {events}"


def test_differ_statistics_with_changes(store):
    """Test that Differ tracks statistics correctly with changes."""
    # Create tree1 with 100 entries
    tree1 = ProllyTree(pattern=0.0001, seed=42, store=store)
    entries1 = [(i, f"v{i}") for i in range(1, 101)]
    tree1.insert_batch(entries1, verbose=False)
    hash1 = tree1._hash_node(tree1.root)

    # Create tree2 with modification in middle
    tree2 = ProllyTree(pattern=0.0001, seed=42, store=store)
    entries2 = [(i, f"v{i}") for i in range(1, 101)]
    # Modify one entry
    entries2[49] = (50, "MODIFIED")
    tree2.insert_batch(entries2, verbose=False)
    hash2 = tree2._hash_node(tree2.root)

    # Use Differ to track stats
    differ = Differ(store)
    events = list(differ.diff(hash1, hash2))

    # Should have one modification
    assert len(events) == 1
    assert isinstance(events[0], Modified)
    assert events[0].key == 50

    # Check statistics
    stats = differ.get_stats()
    # Should have compared at least the root node
    assert stats.nodes_compared >= 1
    # subtrees_skipped can be 0 or more depending on tree structure
    assert stats.subtrees_skipped >= 0
