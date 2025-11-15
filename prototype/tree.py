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
ProllyTree implementation with content-based splitting.

Features:
- Content-addressed nodes (hash of contents)
- Rolling hash-based splitting (Rabin fingerprinting)
- Incremental batch insert with subtree reuse
- Pluggable storage backends via Store protocol
"""

import hashlib
from typing import Optional
from store import Node, Store, MemoryStore


class BatchStats:
    """Statistics for a single batch operation."""
    __slots__ = ('nodes_created', 'leaves_created', 'internals_created',
                 'nodes_reused', 'subtrees_reused', 'nodes_read')

    def __init__(self):
        self.nodes_created = 0
        self.leaves_created = 0
        self.internals_created = 0
        self.nodes_reused = 0
        self.subtrees_reused = 0
        self.nodes_read = 0

    def reset(self):
        """Reset all counters to zero."""
        self.nodes_created = 0
        self.leaves_created = 0
        self.internals_created = 0
        self.nodes_reused = 0
        self.subtrees_reused = 0
        self.nodes_read = 0

    def to_dict(self):
        """Convert to dictionary for backwards compatibility."""
        return {
            'nodes_created': self.nodes_created,
            'leaves_created': self.leaves_created,
            'internals_created': self.internals_created,
            'nodes_reused': self.nodes_reused,
            'subtrees_reused': self.subtrees_reused,
            'nodes_read': self.nodes_read,
        }


class ProllyTree:
    def __init__(self, pattern=0.25, seed=42, store: Optional[Store] = None):
        """
        Initialize ProllyTree with content-based splitting.

        Args:
            pattern: Split probability (0.0 to 1.0). Lower = larger nodes.
                    Default 0.25 means ~4 entries per node on average.
            seed: Seed for rolling hash function for reproducibility
            store: Storage backend (defaults to MemoryStore if not provided)
        """
        self.pattern = int(pattern * (2**32))  # Convert to uint32 threshold
        self.seed = seed
        self.store = store if store is not None else MemoryStore()

        self.root = Node(is_leaf=True)

        # Operation statistics
        self.stats = BatchStats()

    def reset_stats(self):
        """Reset operation statistics for a new batch"""
        self.stats.reset()

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
        existing = self.store.get_node(node_hash)
        if existing is None:
            self.store.put_node(node_hash, node)
            self.stats.nodes_created += 1
            if node.is_leaf:
                self.stats.leaves_created += 1
            else:
                self.stats.internals_created += 1
        else:
            self.stats.nodes_reused += 1

        return node_hash

    def _get_node(self, node_hash):
        """Retrieve node by hash"""
        return self.store.get_node(node_hash)

    def insert_batch(self, mutations, verbose=True):
        """
        Incrementally insert a batch of (key, value) pairs.
        mutations: sorted list of (key, value) tuples
        Returns: dict with operation stats
        """
        # Reset stats for this batch
        self.reset_stats()

        # Track cache stats before this batch (if using CachedFSStore)
        from store import CachedFSStore
        cache_stats_before = None
        if isinstance(self.store, CachedFSStore):
            cache_stats_before = {
                'hits': self.store.cache_hits,
                'misses': self.store.cache_misses
            }

        # Rebuild tree with mutations (always quiet during rebuild)
        new_root = self._rebuild_with_mutations(self.root, mutations, verbose=False)

        # Store the new root (unless it was reused)
        if new_root is not self.root:
            self._store_node(new_root)

        self.root = new_root

        stats = self._summarize_ops()

        # Print single-line batch summary
        if verbose:
            summary_parts = [f"Inserted {len(mutations)} rows"]
            summary_parts.append(f"{stats['nodes_created']} new nodes created")

            # Add cache stats if using CachedFSStore
            if isinstance(self.store, CachedFSStore) and cache_stats_before:
                hits_delta = self.store.cache_hits - cache_stats_before['hits']
                misses_delta = self.store.cache_misses - cache_stats_before['misses']
                summary_parts.append(f"{hits_delta} cache hits")
                summary_parts.append(f"{misses_delta} cache misses")

                # Add average node sizes from CachedFSStore
                size_stats = self.store.get_size_stats()
                if size_stats['avg_leaf_size'] > 0:
                    summary_parts.append(f"avg leaf: {size_stats['avg_leaf_size']:.0f}B")
                if size_stats['avg_internal_size'] > 0:
                    summary_parts.append(f"avg internal: {size_stats['avg_internal_size']:.0f}B")

            print("; ".join(summary_parts))

        return stats

    def _rebuild_with_mutations(self, node, mutations, verbose=True):
        """
        Core incremental rebuild logic.
        Returns: new node (possibly with different structure)
        """
        if verbose:
            node_type = 'Leaf' if node.is_leaf else 'Internal'
            print(f"\n_rebuild_with_mutations: {node_type} node with {len(node.keys)} keys, {len(mutations)} mutations")

        if not mutations:
            # No mutations for this subtree - REUSE it!
            if verbose:
                print(f"  -> No mutations, reusing node")
            return node

        if node.is_leaf:
            # Leaf node: merge old data with mutations
            if verbose:
                print(f"  -> Leaf node, merging {len(node.keys)} existing + {len(mutations)} new entries...")
            merged = self._merge_sorted(
                list(zip(node.keys, node.values)),
                mutations
            )
            if verbose:
                print(f"  -> Merged to {len(merged)} total entries")

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
                print(f"  -> Internal node with {len(node.values)} children, {len(node.keys)} separator keys")

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

    def _print_tree(self, label="", verbose=False):
        """
        Print tree structure for debugging.
        
        Args:
            label: Label to display with the tree
            verbose: If True, print all leaf node values. If False, only show first/last keys and count.
        """
        print(f"\n{'='*60}")
        print(f"TREE {label}:")
        print(f"{'='*60}")
        # For the root, we don't have a hash readily available
        # We'd need to compute it or track it separately
        root_hash = self._hash_node(self.root)
        self._print_node(self.root, root_hash, prefix="", is_last=True, verbose=verbose)

    def _print_node(self, node, node_hash, prefix="", is_last=True, reused_hashes=None, verbose=False):
        """
        Recursively print node and its children.
        
        Args:
            node: Node to print
            node_hash: Hash of the node
            prefix: Prefix for tree formatting
            is_last: Whether this is the last child
            reused_hashes: Set of reused hashes to mark
            verbose: If True, print all leaf node values. If False, only show first/last keys and count.
        """
        branch = "└── " if is_last else "├── "

        # Check if this node was reused
        reused_flag = ""
        if reused_hashes is not None and node_hash is not None and node_hash in reused_hashes:
            reused_flag = " <- REUSED!"

        if node.is_leaf:
            hash_str = f"#{node_hash}" if node_hash is not None else "#root"
            if verbose:
                # Show all key-value pairs
                data = list(zip(node.keys, node.values))
                print(f"{prefix}{branch}LEAF {hash_str}: {data}{reused_flag}")
            else:
                # Show only first and last keys, and the count
                count = len(node.keys)
                if count == 0:
                    print(f"{prefix}{branch}LEAF {hash_str}: (empty){reused_flag}")
                elif count == 1:
                    print(f"{prefix}{branch}LEAF {hash_str}: [{node.keys[0]}] (1 key){reused_flag}")
                else:
                    first_key = node.keys[0]
                    last_key = node.keys[-1]
                    print(f"{prefix}{branch}LEAF {hash_str}: [{first_key} ... {last_key}] ({count} keys){reused_flag}")
        else:
            hash_str = f"#{node_hash}" if node_hash is not None else "#root"
            print(f"{prefix}{branch}INTERNAL {hash_str}: keys={node.keys}{reused_flag}")

            # Print children
            extension = "    " if is_last else "│   "
            for i, child_hash in enumerate(node.values):
                child = self._get_node(child_hash)
                child_is_last = (i == len(node.values) - 1)
                self._print_node(child, child_hash, prefix + extension, child_is_last, reused_hashes, verbose)

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
        return self.stats.to_dict()

    def items(self, prefix=""):
        """
        Generator that yields (key, value) pairs with keys matching the given prefix.

        Navigates to the prefix and yields items until the cursor exceeds the prefix.

        Args:
            prefix: Key prefix to filter (default: "" returns all items)

        Yields:
            Tuples of (key, value) for keys matching the prefix
        """
        yield from self._items_from_node(self.root, prefix)

    def _items_from_node(self, node, prefix):
        """Recursively yield items from node and its children that match prefix"""
        if node.is_leaf:
            # Yield matching leaf entries
            for key, value in zip(node.keys, node.values):
                # Handle both string and non-string keys
                if isinstance(key, str):
                    if key.startswith(prefix):
                        yield (key, value)
                else:
                    # For non-string keys, only yield if prefix is empty
                    if not prefix:
                        yield (key, value)
        else:
            # For internal nodes, traverse children
            for i, child_hash in enumerate(node.values):
                # Get the range this child covers
                # child contains keys: [lower_bound, upper_bound)
                # where lower_bound = node.keys[i-1] (or -inf for i=0)
                # and upper_bound = node.keys[i] (or +inf for last child)

                lower_bound = node.keys[i-1] if i > 0 else (""  if isinstance(prefix, str) else None)
                upper_bound = node.keys[i] if i < len(node.keys) else None

                # Skip if this child is entirely before the prefix (only for string keys)
                if prefix and isinstance(upper_bound, str) and upper_bound <= prefix:
                    continue

                # Skip if this child is entirely after the prefix range (only for string keys)
                if prefix and isinstance(lower_bound, str) and lower_bound > prefix + '\xff':
                    break

                # This child might contain matching keys
                child = self._get_node(child_hash)
                yield from self._items_from_node(child, prefix)

    def verify(self):
        """Verify tree structure and return all key-value pairs in order (for backwards compatibility)"""
        return list(self.items())
