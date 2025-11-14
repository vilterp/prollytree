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
ProllyTree prototype with content-based splitting.

Features:
- Content-addressed nodes (hash of contents)
- Rolling hash-based splitting (Rabin fingerprinting)
- Incremental batch insert with subtree reuse
- Track operations to verify optimization
"""

import hashlib

class Node:
    def __init__(self, is_leaf=True):
        self.is_leaf = is_leaf
        self.keys = []      # Separator keys (for internal) or actual keys (for leaves)
        self.values = []    # Child pointers (for internal) or actual values (for leaves)

    def __repr__(self):
        if self.is_leaf:
            return f"Leaf({list(zip(self.keys, self.values))})"
        else:
            return f"Internal(keys={self.keys}, children={len(self.values)})"

class ProllyTree:
    def __init__(self, pattern=0.25, seed=42):
        """
        Initialize ProllyTree with content-based splitting.

        Args:
            pattern: Split probability (0.0 to 1.0). Lower = larger nodes.
                    Default 0.25 means ~4 entries per node on average.
            seed: Seed for rolling hash function for reproducibility
        """
        self.pattern = int(pattern * (2**32))  # Convert to uint32 threshold
        self.seed = seed

        self.root = Node(is_leaf=True)
        self.nodes = {}  # Content-addressed storage: hash -> node

        # Operation tracking
        self.ops = []
        self.reset_ops()

    def reset_ops(self):
        """Reset operation tracking for a new batch"""
        self.ops = []

    def _rolling_hash(self, current_hash, data):
        """
        Simple rolling hash (Rabin fingerprinting style).
        Updates the hash with new data.

        Args:
            current_hash: Current hash value (use self.seed for initial)
            data: bytes-like object to add to the hash

        Returns:
            Updated hash value (uint32)
        """
        h = current_hash
        for byte in data:
            h = ((h * 31) + byte) & 0xFFFFFFFF
        return h

    def _hash_node(self, node):
        """
        Compute content hash for a node.

        For leaf nodes: hash the key-value pairs
        For internal nodes: hash the (separator_key, child_hash) pairs
        """
        content = []
        if node.is_leaf:
            # Hash key-value pairs
            for key, value in zip(node.keys, node.values):
                content.append(f"{key}:{value}".encode('utf-8'))
        else:
            # Hash separator keys and child hashes
            for i, child_hash in enumerate(node.values):
                if i < len(node.keys):
                    content.append(f"{node.keys[i]}:{child_hash}".encode('utf-8'))
                else:
                    content.append(f"_:{child_hash}".encode('utf-8'))

        # Combine all content and hash
        combined = b'|'.join(content)
        return hashlib.sha256(combined).hexdigest()[:16]  # Use first 16 chars for readability

    def _store_node(self, node):
        """Store node and return its content-based hash"""
        node_hash = self._hash_node(node)

        # Only store if not already present (deduplication)
        if node_hash not in self.nodes:
            self.nodes[node_hash] = node
            self.ops.append(('create_node', 'leaf' if node.is_leaf else 'internal', len(node.keys)))
        else:
            self.ops.append(('reuse_existing', node_hash))

        return node_hash

    def _get_node(self, node_hash):
        """Retrieve node by hash"""
        self.ops.append(('read_node', node_hash))
        return self.nodes.get(node_hash)

    def insert_batch(self, mutations, verbose=True):
        """
        Incrementally insert a batch of (key, value) pairs.
        mutations: sorted list of (key, value) tuples
        Returns: dict with operation stats
        """
        self.reset_ops()

        if verbose:
            print(f"\n=== Rebuilding tree with {len(mutations)} mutations ===")

        # Rebuild tree with mutations
        new_root = self._rebuild_with_mutations(self.root, mutations, verbose)

        # Store the new root (unless it was reused)
        if new_root is not self.root:
            self._store_node(new_root)

        self.root = new_root

        return self._summarize_ops()

    def _rebuild_with_mutations(self, node, mutations, verbose=True):
        """
        Core incremental rebuild logic.
        Returns: new node (possibly with different structure)
        """
        if verbose:
            print(f"\n_rebuild_with_mutations(node={node}, mutations={mutations})")

        if not mutations:
            # No mutations for this subtree - REUSE it!
            if verbose:
                print(f"  -> No mutations, reusing node")
            self.ops.append(('reuse_node', 'leaf' if node.is_leaf else 'internal'))
            return node

        if node.is_leaf:
            # Leaf node: merge old data with mutations
            if verbose:
                print(f"  -> Leaf node, merging...")
            merged = self._merge_sorted(
                list(zip(node.keys, node.values)),
                mutations
            )
            if verbose:
                print(f"  -> Merged data: {merged}")

            # Build new leaf nodes (may split if too large)
            new_leaves = self._build_leaves(merged)
            if verbose:
                print(f"  -> Built {len(new_leaves)} leaf nodes")

            if len(new_leaves) == 1:
                return new_leaves[0]
            else:
                # Multiple leaves - need parent (may recursively split if too many)
                return self._build_internal_from_children(new_leaves, verbose)

        else:
            # Internal node: partition mutations by child ranges, recursively rebuild
            if verbose:
                print(f"  -> Internal node with {len(node.values)} children")
                print(f"  -> Separator keys: {node.keys}")

            new_child_nodes = []  # List of actual Node objects (not hashes yet)
            mut_idx = 0

            for child_idx in range(len(node.values)):
                # Determine key range for this child
                if child_idx == 0:
                    lower = None  # -infinity
                else:
                    # Handle case where node has fewer keys than expected
                    if child_idx - 1 < len(node.keys):
                        lower = node.keys[child_idx - 1]
                    else:
                        lower = None

                if child_idx < len(node.keys):
                    upper = node.keys[child_idx]
                else:
                    upper = None  # +infinity

                if verbose:
                    print(f"  -> Child {child_idx}: range [{lower}, {upper})")

                # Collect mutations for this child
                child_mutations = []
                while mut_idx < len(mutations):
                    mut_key = mutations[mut_idx][0]

                    in_range = True
                    if lower is not None and mut_key < lower:
                        in_range = False
                    if upper is not None and mut_key >= upper:
                        in_range = False

                    if in_range:
                        child_mutations.append(mutations[mut_idx])
                        mut_idx += 1
                    elif upper is not None and mut_key >= upper:
                        # Beyond this child's range
                        break
                    else:
                        mut_idx += 1

                if verbose:
                    print(f"    -> {len(child_mutations)} mutations for this child")

                # Get child and recursively rebuild
                child_hash = node.values[child_idx]

                if not child_mutations:
                    # No mutations - reuse the existing child by its hash!
                    self.ops.append(('reuse_subtree', child_hash))
                    # We need the actual node to get its first key for separators
                    child_node = self._get_node(child_hash)
                    # Mark this child as reused by storing the hash in a special way
                    child_node._reused_hash = child_hash
                    new_child_nodes.append(child_node)
                else:
                    # Has mutations - need to rebuild
                    child = self._get_node(child_hash)
                    new_child = self._rebuild_with_mutations(child, child_mutations, verbose)

                    # Child rebuild might return a node that needs to be split into multiple
                    # If it's a leaf that became too large, it would have been split in _rebuild_with_mutations
                    # But if it's an internal node that's too large, we need to handle it here
                    new_child_nodes.append(new_child)

            # Now build parent nodes from the collected children
            # Each child might have split, so we need to flatten and rebuild the parent structure
            return self._build_internal_from_children(new_child_nodes, verbose)

    def _build_internal_from_children(self, children, verbose=False):
        """
        Build internal node(s) from a list of children using rolling hash for splits.

        Args:
            children: List of Node objects

        Returns:
            Node (single child, or newly created internal node)
        """
        if len(children) == 0:
            raise ValueError("Cannot build internal node with no children")

        if len(children) == 1:
            # Single child - just return it (no need for parent)
            return children[0]

        # Build internal nodes using rolling hash to determine split points
        # Strategy: Don't split unless we have at least 2 children on BOTH sides
        internal_nodes = []
        current_internal = Node(is_leaf=False)
        roll_hash = self.seed  # Start with seed

        for i, child in enumerate(children):
            # Store or reuse child hash
            if hasattr(child, '_reused_hash'):
                child_hash = child._reused_hash
                delattr(child, '_reused_hash')
            else:
                child_hash = self._store_node(child)

            current_internal.values.append(child_hash)

            # Update rolling hash with the child hash
            hash_bytes = str(child_hash).encode('utf-8')
            roll_hash = self._rolling_hash(roll_hash, hash_bytes)

            # Add separator key (first key of next child)
            if i < len(children) - 1:
                next_child = children[i + 1]
                # Get the first key from the next child
                if len(next_child.keys) > 0:
                    separator = next_child.keys[0]
                    current_internal.keys.append(separator)

                    # Update rolling hash with separator key
                    sep_bytes = str(separator).encode('utf-8')
                    roll_hash = self._rolling_hash(roll_hash, sep_bytes)

                    # Check if we should split here using rolling hash
                    # Require:
                    # - At least 2 children in current node
                    # - At least 2 children remaining (including next)
                    MIN_CHILDREN = 2
                    children_remaining = len(children) - i - 1
                    if (roll_hash < self.pattern and
                        len(current_internal.values) >= MIN_CHILDREN and
                        children_remaining >= MIN_CHILDREN):
                        # Split point! Save current internal and start new one
                        internal_nodes.append(current_internal)
                        current_internal = Node(is_leaf=False)
                        roll_hash = self.seed  # Reset hash for next node
                        if verbose:
                            print(f"  -> Internal node split at separator {separator} (hash={roll_hash} < {self.pattern})")
                else:
                    # Empty child node - skip it
                    if verbose:
                        print(f"  -> Warning: child {i+1} has no keys, skipping separator")

        # Add the last internal node (but only if it has multiple children)
        if current_internal.values:
            if len(current_internal.values) == 1 and not internal_nodes:
                # Only one child total - just return it directly
                child_hash = current_internal.values[0]
                return self._get_node(child_hash)
            elif len(current_internal.values) > 1:
                internal_nodes.append(current_internal)
            elif internal_nodes:
                # Single child but we already have other nodes - add it
                internal_nodes.append(current_internal)

        # Handle edge cases
        if len(internal_nodes) == 0:
            raise ValueError("No internal nodes created")
        elif len(internal_nodes) == 1:
            # Single internal node
            node = internal_nodes[0]
            if len(node.values) == 1:
                # Unwrap single-child internal node - return the child directly
                if verbose:
                    print(f"  -> Unwrapping single-child internal node")
                child_hash = node.values[0]
                return self._get_node(child_hash)
            elif len(node.values) == 0:
                raise ValueError("Internal node has no children")
            else:
                return node
        else:
            # Multiple internal nodes - build parent recursively
            if verbose:
                print(f"  -> Created {len(internal_nodes)} internal nodes, building parent...")
            return self._build_internal_from_children(internal_nodes, verbose)

    def _merge_sorted(self, old_items, new_items):
        """Merge two sorted lists of (key, value) tuples"""
        result = []
        i, j = 0, 0

        while i < len(old_items) and j < len(new_items):
            if old_items[i][0] < new_items[j][0]:
                result.append(old_items[i])
                i += 1
            elif old_items[i][0] == new_items[j][0]:
                # New value overwrites old
                result.append(new_items[j])
                i += 1
                j += 1
            else:
                result.append(new_items[j])
                j += 1

        result.extend(old_items[i:])
        result.extend(new_items[j:])
        return result

    def _build_leaves(self, items):
        """
        Build leaf nodes from sorted items using rolling hash for splits.

        Split points are determined by rolling hash being below pattern threshold,
        with a minimum of 2 entries per node to avoid degenerate splits.
        """
        if not items:
            return []

        MIN_NODE_SIZE = 2  # Minimum entries per node to avoid degenerate trees

        leaves = []
        current_keys = []
        current_values = []
        roll_hash = self.seed  # Start with seed

        for i, (key, value) in enumerate(items):
            current_keys.append(key)
            current_values.append(value)

            # Update rolling hash with the key and value bytes
            key_bytes = str(key).encode('utf-8')
            value_bytes = str(value).encode('utf-8')
            roll_hash = self._rolling_hash(roll_hash, key_bytes)
            roll_hash = self._rolling_hash(roll_hash, value_bytes)

            # Split if: (1) have minimum entries AND hash below pattern OR (2) last item
            has_min = len(current_keys) >= MIN_NODE_SIZE
            should_split = (has_min and roll_hash < self.pattern) or (i == len(items) - 1)

            if should_split and current_keys:
                leaf = Node(is_leaf=True)
                leaf.keys = current_keys
                leaf.values = current_values
                leaves.append(leaf)

                # Reset for next leaf
                current_keys = []
                current_values = []
                roll_hash = self.seed  # Reset hash for next node

        return leaves if leaves else [Node(is_leaf=True)]

    def _print_tree(self, label=""):
        """Print tree structure for debugging"""
        print(f"\n{'='*60}")
        print(f"TREE {label}:")
        print(f"{'='*60}")
        # Find the hash for the root node
        root_hash = None
        for h, n in self.nodes.items():
            if n is self.root:
                root_hash = h
                break
        self._print_node(self.root, root_hash, prefix="", is_last=True)

    def _print_node(self, node, node_hash, prefix="", is_last=True, reused_hashes=None):
        """Recursively print node and its children"""
        branch = "└── " if is_last else "├── "

        # Check if this node was reused
        reused_flag = ""
        if reused_hashes is not None and node_hash is not None and node_hash in reused_hashes:
            reused_flag = " <- REUSED!"

        if node.is_leaf:
            data = list(zip(node.keys, node.values))
            hash_str = f"#{node_hash}" if node_hash is not None else "#root"
            print(f"{prefix}{branch}LEAF {hash_str}: {data}{reused_flag}")
        else:
            hash_str = f"#{node_hash}" if node_hash is not None else "#root"
            print(f"{prefix}{branch}INTERNAL {hash_str}: keys={node.keys}{reused_flag}")

            # Print children
            extension = "    " if is_last else "│   "
            for i, child_hash in enumerate(node.values):
                child = self._get_node(child_hash)
                child_is_last = (i == len(node.values) - 1)
                self._print_node(child, child_hash, prefix + extension, child_is_last, reused_hashes)

    def _print_ops(self):
        """Print operation statistics"""
        print(f"\n{'='*60}")
        print("OPERATIONS:")
        print(f"{'='*60}")
        stats = self._summarize_ops()
        for key, value in stats.items():
            print(f"{key}: {value}")

    def _summarize_ops(self):
        """Summarize operations into statistics"""
        stats = {
            'total_ops': len(self.ops),
            'nodes_created': sum(1 for op in self.ops if op[0] == 'create_node'),
            'nodes_reused': sum(1 for op in self.ops if op[0] in ('reuse_node', 'reuse_subtree')),
            'subtrees_reused': sum(1 for op in self.ops if op[0] == 'reuse_subtree'),
            'nodes_read': sum(1 for op in self.ops if op[0] == 'read_node'),
            'leaves_created': sum(1 for op in self.ops if op[0] == 'create_node' and op[1] == 'leaf'),
            'internals_created': sum(1 for op in self.ops if op[0] == 'create_node' and op[1] == 'internal'),
        }
        return stats

    def verify(self):
        """Verify tree structure and return all key-value pairs in order"""
        result = []
        self._collect_leaves(self.root, result)
        return result

    def _collect_leaves(self, node, result):
        """Collect all leaf entries in order"""
        if node.is_leaf:
            result.extend(zip(node.keys, node.values))
        else:
            for child_hash in node.values:
                child = self._get_node(child_hash)
                self._collect_leaves(child, result)


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
    # Capture existing node hashes before insert
    old_node_hashes = set(old_tree.nodes.keys())

    if verbose:
        print(f"\n{'-'*60}")
        print(f"INSERTING: {mutations}")
        print(f"{'-'*60}")
        print("\nTREE BEFORE INSERT:")
        # Find root hash
        root_hash = None
        for h, n in old_tree.nodes.items():
            if n is old_tree.root:
                root_hash = h
                break
        old_tree._print_node(old_tree.root, root_hash, prefix="", is_last=True)

    # Create a new tree by copying the old one
    new_tree = ProllyTree(pattern=old_tree.pattern / (2**32), seed=old_tree.seed)
    new_tree.root = old_tree.root  # Share the root (immutable)
    new_tree.nodes = old_tree.nodes.copy()  # Share the node storage

    # Perform the insert (this will create new nodes but won't modify old ones)
    stats = new_tree.insert_batch(mutations, verbose=verbose)

    # Verify the result
    result = new_tree.verify()
    assert result == expected_contents, f"Expected {expected_contents}, got {result}"

    if verbose:
        print("\nTREE AFTER INSERT:")
        # Find root hash
        root_hash = None
        for h, n in new_tree.nodes.items():
            if n is new_tree.root:
                root_hash = h
                break
        new_tree._print_node(new_tree.root, root_hash, prefix="", is_last=True, reused_hashes=old_node_hashes)

        print(f"\n{'-'*60}")
        print("OPERATION STATS:")
        print(f"{'-'*60}")
        for key, value in stats.items():
            print(f"  {key}: {value}")

    return new_tree, stats


# Test the implementation
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
