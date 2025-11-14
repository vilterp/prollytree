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
Simplified ProllyTree prototype to understand incremental batch insert.

Key simplifications:
- Fixed-size nodes (max 4 keys per node)
- No rolling hash (just split when full)
- Focus on the incremental rebuild logic
- Track operations to verify optimization
"""

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
    MAX_KEYS = 4  # Simplified: split when node has more than 4 keys

    def __init__(self):
        self.root = Node(is_leaf=True)
        self.nodes = {}  # Simulate content-addressed storage: hash -> node
        self.next_hash = 0

        # Operation tracking
        self.ops = []  # List of operations performed
        self.reset_ops()

    def reset_ops(self):
        """Reset operation tracking for a new batch"""
        self.ops = []

    def _store_node(self, node):
        """Store node and return its hash"""
        node_hash = self.next_hash
        self.next_hash += 1
        self.nodes[node_hash] = node
        self.ops.append(('create_node', 'leaf' if node.is_leaf else 'internal', len(node.keys)))
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
                    lower = node.keys[child_idx - 1]

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
        Build internal node(s) from a list of children.
        Handles splitting if the internal node would exceed MAX_KEYS.

        Args:
            children: List of Node objects

        Returns:
            Node or list of Nodes if this level needs to split too
        """
        if len(children) == 0:
            raise ValueError("Cannot build internal node with no children")

        if len(children) == 1:
            # Single child - just return it (no need for parent)
            return children[0]

        # Store children and create separator keys
        child_hashes = []
        separator_keys = []

        for i, child in enumerate(children):
            # Check if this child was reused (has a hash already)
            if hasattr(child, '_reused_hash'):
                child_hash = child._reused_hash
                delattr(child, '_reused_hash')  # Clean up the marker
            else:
                child_hash = self._store_node(child)

            child_hashes.append(child_hash)

            # Separator key is the first key of the next child
            if i < len(children) - 1:
                next_child = children[i + 1]
                separator = next_child.keys[0]
                separator_keys.append(separator)

        # Check if we need to split this internal node
        if len(separator_keys) <= self.MAX_KEYS:
            # Fits in one internal node
            internal = Node(is_leaf=False)
            internal.keys = separator_keys
            internal.values = child_hashes
            return internal
        else:
            # Too many children - need to split into multiple internal nodes
            if verbose:
                print(f"  -> Internal node too large ({len(separator_keys)} keys), splitting...")

            # Split children into groups
            internal_nodes = []
            chunk_size = self.MAX_KEYS + 1  # +1 because we have one more child than separator keys

            for i in range(0, len(children), chunk_size):
                chunk_children = children[i:i+chunk_size]

                # Build internal node for this chunk
                internal = Node(is_leaf=False)
                for j, child in enumerate(chunk_children):
                    # Store child
                    if hasattr(child, '_reused_hash'):
                        child_hash = child._reused_hash
                        delattr(child, '_reused_hash')
                    else:
                        child_hash = self._store_node(child)

                    internal.values.append(child_hash)

                    # Separator key
                    if j < len(chunk_children) - 1:
                        next_child = chunk_children[j + 1]
                        separator = next_child.keys[0]
                        internal.keys.append(separator)

                internal_nodes.append(internal)

            # Recursively build parent for these internal nodes
            if len(internal_nodes) == 1:
                return internal_nodes[0]
            else:
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
        """Build leaf nodes from sorted items, splitting when needed"""
        if len(items) <= self.MAX_KEYS:
            # Fits in single leaf
            leaf = Node(is_leaf=True)
            leaf.keys = [k for k, v in items]
            leaf.values = [v for k, v in items]
            return [leaf]

        # Split into multiple leaves
        leaves = []
        for i in range(0, len(items), self.MAX_KEYS):
            chunk = items[i:i+self.MAX_KEYS]
            leaf = Node(is_leaf=True)
            leaf.keys = [k for k, v in chunk]
            leaf.values = [v for k, v in chunk]
            leaves.append(leaf)

        return leaves

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
    new_tree = ProllyTree()
    new_tree.root = old_tree.root  # Share the root (immutable)
    new_tree.nodes = old_tree.nodes.copy()  # Share the node storage
    new_tree.next_hash = old_tree.next_hash

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
    tree0 = ProllyTree()

    print("\n" + "="*80)
    print("TEST 1: Insert batch into empty tree")
    print("="*80)
    tree1, stats1 = test_insert(
        tree0,
        mutations=[(i, f"v{i}") for i in [2, 4, 6, 8, 10, 12]],
        expected_contents=[(i, f"v{i}") for i in [2, 4, 6, 8, 10, 12]],
        verbose=True
    )
    # First insert into empty tree: should create 2 leaves + 1 parent = 3 nodes
    assert stats1['nodes_created'] == 3, f"Expected 3 nodes created, got {stats1['nodes_created']}"
    assert stats1['nodes_reused'] == 0, f"Expected 0 nodes reused, got {stats1['nodes_reused']}"
    print("✓ TEST 1 PASSED")

    print("\n" + "="*80)
    print("TEST 2: Insert batch with interleaved keys")
    print("="*80)
    tree2, stats2 = test_insert(
        tree1,
        mutations=[(i, f"v{i}") for i in [1, 3, 5, 7, 9, 11]],
        expected_contents=[(i, f"v{i}") for i in range(1, 13)],
        verbose=True
    )
    # Second insert: mutations affect both children (left gets 5 mutations, right gets 1)
    # Should NOT reuse any nodes because both children are affected
    assert stats2['nodes_reused'] == 0, f"Expected 0 nodes reused, got {stats2['nodes_reused']}"
    print("✓ TEST 2 PASSED")

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
    # The left subtree should be reused!
    assert stats3['subtrees_reused'] == 1, f"Expected 1 subtree reused, got {stats3['subtrees_reused']}"
    print("✓ TEST 3 PASSED - Left subtree was reused!")

    print("\n" + "="*80)
    print("TEST 4: Large insert causing internal node split")
    print("="*80)
    # Insert many more keys to cause internal nodes to split
    # With MAX_KEYS=4, we need more than 4 children in an internal node to force a split
    # Current tree has 2 top-level children. Let's add many keys to create more leaf splits
    tree4, stats4 = test_insert(
        tree3,
        mutations=[(i, f"v{i}") for i in range(17, 41)],  # Add 24 more keys (17-40)
        expected_contents=[(i, f"v{i}") for i in range(1, 41)],
        verbose=True
    )
    print("✓ TEST 4 PASSED - Internal node splitting handled!")

    print("\n" + "="*80)
    print("ALL TESTS PASSED!")
    print("="*80)
