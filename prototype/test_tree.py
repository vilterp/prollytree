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

from tree import ProllyTree
from store import MemoryStore


def test_insert(old_tree, mutations, expected_contents, verbose=True):
    """
    Helper function to test batch insert (functional style).

    Args:
        old_tree: Current ProllyTree instance (will not be modified)
        mutations: List of (key, value) tuples to insert
        expected_contents: Expected list of (key, value) after insert
        verbose: Whether to print detailed output

    Returns:
        (new_tree, stats): New ProllyTree instance and operation statistics
    """
    # Capture existing node hashes before insert (if using MemoryStore)
    old_node_hashes = set()
    if isinstance(old_tree.store, MemoryStore):
        old_node_hashes = set(old_tree.store.nodes.keys())

    if verbose:
        print(f"\n{'-'*60}")
        print(f"INSERTING: {mutations}")
        print(f"{'-'*60}")
        print("\nTREE BEFORE INSERT:")
        root_hash = old_tree._hash_node(old_tree.root)
        old_tree._print_node(old_tree.root, root_hash, prefix="", is_last=True)

    # Create a new tree that shares the same store
    new_tree = ProllyTree(pattern=old_tree.pattern / (2**32), seed=old_tree.seed, store=old_tree.store)
    new_tree.root = old_tree.root  # Share the root (immutable)

    # Perform the insert (this will create new nodes but won't modify old ones)
    stats = new_tree.insert_batch(mutations, verbose=verbose)

    # Verify the result
    result = new_tree.verify()
    assert result == expected_contents, f"Expected {expected_contents}, got {result}"

    if verbose:
        print("\nTREE AFTER INSERT:")
        root_hash = new_tree._hash_node(new_tree.root)
        new_tree._print_node(new_tree.root, root_hash, prefix="", is_last=True, reused_hashes=old_node_hashes)

        print(f"\n{'-'*60}")
        print("OPERATION STATS:")
        print(f"{'-'*60}")
        for key, value in stats.items():
            print(f"  {key}: {value}")

    return new_tree, stats


if __name__ == "__main__":
    # Use a low pattern for more predictable splitting in tests
    # pattern=0.0001 means split when hash < 429,497 (out of 4,294,967,296)
    tree0 = ProllyTree(pattern=0.0001, seed=42)

    print("\n" + "="*80)
    print("TEST 1: Insert batch into empty tree")
    print("="*80)
    tree1, stats1 = test_insert(
        tree0,
        mutations=[(i, f"v{i}") for i in [2, 4, 6, 8, 10, 12]],
        expected_contents=[(i, f"v{i}") for i in [2, 4, 6, 8, 10, 12]],
        verbose=True
    )
    # With rolling hash, node count may vary - just check it completed
    print(f"✓ TEST 1 PASSED (created {stats1['nodes_created']} nodes)")

    print("\n" + "="*80)
    print("TEST 2: Insert batch with interleaved keys")
    print("="*80)
    tree2, stats2 = test_insert(
        tree1,
        mutations=[(i, f"v{i}") for i in [1, 3, 5, 7, 9, 11]],
        expected_contents=[(i, f"v{i}") for i in range(1, 13)],
        verbose=True
    )
    print(f"✓ TEST 2 PASSED (created {stats2['nodes_created']} nodes)")

    print("\n" + "="*80)
    print("TEST 3: Insert batch with keys in unaffected range")
    print("="*80)
    # Insert keys > 12, which should only affect the right subtree
    tree3, stats3 = test_insert(
        tree2,
        mutations=[(i, f"v{i}") for i in [13, 14, 15, 16]],
        expected_contents=[(i, f"v{i}") for i in range(1, 17)],
        verbose=True
    )
    # Check if we reused any subtrees
    if stats3['subtrees_reused'] > 0:
        print(f"✓ TEST 3 PASSED - {stats3['subtrees_reused']} subtree(s) reused!")
    else:
        print(f"✓ TEST 3 PASSED (no subtree reuse, different splits)")

    print("\n" + "="*80)
    print("TEST 4: Large insert causing more splits")
    print("="*80)
    # Insert many more keys to cause internal nodes to split
    tree4, stats4 = test_insert(
        tree3,
        mutations=[(i, f"v{i}") for i in range(17, 41)],  # Add 24 more keys (17-40)
        expected_contents=[(i, f"v{i}") for i in range(1, 41)],
        verbose=True
    )
    print(f"✓ TEST 4 PASSED (created {stats4['nodes_created']} nodes, reused {stats4['subtrees_reused']} subtrees)")

    print("\n" + "="*80)
    print("ALL TESTS PASSED!")
    print("="*80)
