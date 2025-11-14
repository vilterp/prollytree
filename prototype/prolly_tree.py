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
            print(f"\n=== INSERT BATCH: {len(mutations)} mutations ===")
            print(f"Mutations: {mutations}")
            self._print_tree("BEFORE")

        # Rebuild tree with mutations
        new_root = self._rebuild_with_mutations(self.root, mutations, verbose)

        # Store the new root (unless it was reused)
        if new_root is not self.root:
            self._store_node(new_root)

        self.root = new_root

        if verbose:
            self._print_tree("AFTER")
            self._print_ops()

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
                # Multiple leaves - need parent
                return self._build_parent(new_leaves)

        else:
            # Internal node: partition mutations by child ranges, recursively rebuild
            if verbose:
                print(f"  -> Internal node with {len(node.values)} children")
                print(f"  -> Separator keys: {node.keys}")

            new_children = []
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
                    # No mutations - reuse the existing child hash!
                    self.ops.append(('reuse_subtree', child_hash))
                    new_children.append(child_hash)
                else:
                    # Has mutations - need to rebuild
                    child = self._get_node(child_hash)
                    new_child = self._rebuild_with_mutations(child, child_mutations, verbose)

                    # Store the new child and use its hash
                    new_child_hash = self._store_node(new_child)
                    new_children.append(new_child_hash)

            # Build new internal node with new children
            # For now, keep same structure (same separator keys)
            # TODO: Handle case where children split and we need new separators
            new_internal = Node(is_leaf=False)
            new_internal.keys = node.keys[:]
            new_internal.values = new_children

            return new_internal

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

    def _build_parent(self, children):
        """Build parent node for list of children"""
        parent = Node(is_leaf=False)

        # Store children and create separator keys
        for i, child in enumerate(children):
            child_hash = self._store_node(child)
            parent.values.append(child_hash)

            # Separator key is the first key of the next child
            if i < len(children) - 1:
                next_child = children[i + 1]
                separator = next_child.keys[0]
                parent.keys.append(separator)

        return parent

    def _print_tree(self, label=""):
        """Print tree structure for debugging"""
        print(f"\n{'='*60}")
        print(f"TREE {label}:")
        print(f"{'='*60}")
        self._print_node(self.root, prefix="", is_last=True)

    def _print_node(self, node, prefix="", is_last=True):
        """Recursively print node and its children"""
        branch = "└── " if is_last else "├── "

        if node.is_leaf:
            data = list(zip(node.keys, node.values))
            print(f"{prefix}{branch}LEAF: {data}")
        else:
            print(f"{prefix}{branch}INTERNAL: keys={node.keys}")

            # Print children
            extension = "    " if is_last else "│   "
            for i, child_hash in enumerate(node.values):
                child = self._get_node(child_hash)
                child_is_last = (i == len(node.values) - 1)
                self._print_node(child, prefix + extension, child_is_last)

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
    if verbose:
        print(f"\n{'='*60}")
        print(f"INSERTING: {mutations}")
        print(f"{'='*60}")
        print("\nTREE BEFORE INSERT:")
        old_tree._print_node(old_tree.root, prefix="", is_last=True)

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
        new_tree._print_node(new_tree.root, prefix="", is_last=True)

        print(f"\n{'='*60}")
        print("OPERATION STATS:")
        print(f"{'='*60}")
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # Verify old tree is unchanged
        print(f"\n{'='*60}")
        print("VERIFYING OLD TREE UNCHANGED:")
        print(f"{'='*60}")
        print("\nOLD TREE (should be unchanged):")
        old_tree._print_node(old_tree.root, prefix="", is_last=True)

    return new_tree, stats


# Test the implementation
if __name__ == "__main__":
    tree0 = ProllyTree()

    print("\n" + "="*60)
    print("TEST 1: Insert batch into empty tree")
    print("="*60)
    tree1, stats1 = test_insert(
        tree0,
        mutations=[(i, f"v{i}") for i in [2, 4, 6, 8, 10, 12]],
        expected_contents=[(i, f"v{i}") for i in [2, 4, 6, 8, 10, 12]],
        verbose=False
    )
    # First insert into empty tree: should create 2 leaves + 1 parent = 3 nodes
    assert stats1['nodes_created'] == 3, f"Expected 3 nodes created, got {stats1['nodes_created']}"
    assert stats1['nodes_reused'] == 0, f"Expected 0 nodes reused, got {stats1['nodes_reused']}"
    print("✓ TEST 1 PASSED")

    print("\n" + "="*60)
    print("TEST 2: Insert batch with interleaved keys")
    print("="*60)
    tree2, stats2 = test_insert(
        tree1,
        mutations=[(i, f"v{i}") for i in [1, 3, 5, 7, 9, 11]],
        expected_contents=[(i, f"v{i}") for i in range(1, 13)],
        verbose=False
    )
    # Second insert: mutations affect both children (left gets 5 mutations, right gets 1)
    # Should NOT reuse any nodes because both children are affected
    assert stats2['nodes_reused'] == 0, f"Expected 0 nodes reused, got {stats2['nodes_reused']}"
    print("✓ TEST 2 PASSED")

    print("\n" + "="*60)
    print("TEST 3: Insert batch with keys in unaffected range")
    print("="*60)
    # Insert keys > 12, which should only affect the right subtree
    tree3, stats3 = test_insert(
        tree2,
        mutations=[(i, f"v{i}") for i in [13, 14, 15, 16]],
        expected_contents=[(i, f"v{i}") for i in range(1, 17)],
        verbose=True  # Show this one in detail
    )
    # The left subtree should be reused!
    assert stats3['subtrees_reused'] == 1, f"Expected 1 subtree reused, got {stats3['subtrees_reused']}"
    print("✓ TEST 3 PASSED - Left subtree was reused!")

    print("\n" + "="*60)
    print("ALL TESTS PASSED!")
    print("="*60)
