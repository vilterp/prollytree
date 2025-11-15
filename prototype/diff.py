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
Diff algorithm for ProllyTree.

Efficiently computes differences between two trees by skipping identical subtrees
based on content hashes.
"""

from dataclasses import dataclass
from typing import Any, Iterator, Union
from store import Store


@dataclass(frozen=True)
class Added:
    """A key-value pair was added."""
    key: Any
    value: Any

    def __repr__(self):
        return f"Added({self.key!r}, {self.value!r})"


@dataclass(frozen=True)
class Deleted:
    """A key was deleted."""
    key: Any

    def __repr__(self):
        return f"Deleted({self.key!r})"


@dataclass(frozen=True)
class Modified:
    """A key's value was modified."""
    key: Any
    old_value: Any
    new_value: Any

    def __repr__(self):
        return f"Modified({self.key!r}, {self.old_value!r} -> {self.new_value!r})"


DiffEvent = Union[Added, Deleted, Modified]


def diff(store: Store, old_hash: str, new_hash: str) -> Iterator[DiffEvent]:
    """
    Compute differences between two trees.

    Args:
        store: Storage backend containing both trees
        old_hash: Root hash of the old tree
        new_hash: Root hash of the new tree

    Yields:
        DiffEvent objects (Added, Deleted, or Modified) in key order
    """
    # If hashes are the same, trees are identical - no diff needed
    if old_hash == new_hash:
        return

    old_node = store.get_node(old_hash)
    new_node = store.get_node(new_hash)

    if old_node is None and new_node is None:
        return
    elif old_node is None:
        # All entries in new_node are additions
        yield from _yield_all_additions(store, new_node)
    elif new_node is None:
        # All entries in old_node are deletions
        yield from _yield_all_deletions(store, old_node)
    else:
        # Both nodes exist - compute diff
        yield from _diff_nodes(store, old_node, new_node)


def _diff_nodes(store: Store, old_node, new_node) -> Iterator[DiffEvent]:
    """
    Recursively diff two nodes.

    Args:
        store: Storage backend
        old_node: Old tree node
        new_node: New tree node

    Yields:
        DiffEvent objects in key order
    """
    if old_node.is_leaf and new_node.is_leaf:
        # Both are leaves - compare entries directly
        yield from _diff_leaves(old_node, new_node)
    elif old_node.is_leaf and not new_node.is_leaf:
        # Old is leaf, new is internal - handle mixed case
        # Get all entries from old leaf
        old_entries = dict(zip(old_node.keys, old_node.values))

        # Traverse new internal node and compare
        yield from _diff_leaf_vs_internal(store, old_entries, new_node, is_old_leaf=True)
    elif not old_node.is_leaf and new_node.is_leaf:
        # Old is internal, new is leaf - handle mixed case
        # Get all entries from new leaf
        new_entries = dict(zip(new_node.keys, new_node.values))

        # Traverse old internal node and compare
        yield from _diff_internal_vs_leaf(store, old_node, new_entries)
    else:
        # Both are internal nodes - traverse in parallel
        yield from _diff_internal_nodes(store, old_node, new_node)


def _diff_leaves(old_node, new_node) -> Iterator[DiffEvent]:
    """
    Diff two leaf nodes.

    Args:
        old_node: Old leaf node
        new_node: New leaf node

    Yields:
        DiffEvent objects in key order
    """
    old_entries = dict(zip(old_node.keys, old_node.values))
    new_entries = dict(zip(new_node.keys, new_node.values))

    # All keys from both leaves
    all_keys = sorted(set(old_entries.keys()) | set(new_entries.keys()))

    for key in all_keys:
        old_has = key in old_entries
        new_has = key in new_entries

        if old_has and new_has:
            # Key exists in both
            if old_entries[key] != new_entries[key]:
                yield Modified(key, old_entries[key], new_entries[key])
        elif new_has:
            # Only in new
            yield Added(key, new_entries[key])
        else:
            # Only in old
            yield Deleted(key)


def _diff_internal_nodes(store: Store, old_node, new_node) -> Iterator[DiffEvent]:
    """
    Diff two internal nodes by traversing children in parallel.

    Args:
        store: Storage backend
        old_node: Old internal node
        new_node: New internal node

    Yields:
        DiffEvent objects in key order
    """
    # Build child ranges for both nodes
    old_children = _get_child_ranges(old_node)
    new_children = _get_child_ranges(new_node)

    old_idx = 0
    new_idx = 0

    while old_idx < len(old_children) or new_idx < len(new_children):
        if old_idx >= len(old_children):
            # No more old children - all remaining new children are additions
            new_hash, _, _ = new_children[new_idx]
            new_child = store.get_node(new_hash)
            yield from _yield_all_additions(store, new_child)
            new_idx += 1
        elif new_idx >= len(new_children):
            # No more new children - all remaining old children are deletions
            old_hash, _, _ = old_children[old_idx]
            old_child = store.get_node(old_hash)
            yield from _yield_all_deletions(store, old_child)
            old_idx += 1
        else:
            old_hash, old_lower, old_upper = old_children[old_idx]
            new_hash, new_lower, new_upper = new_children[new_idx]

            # Check if ranges overlap
            if old_upper is not None and new_lower is not None and old_upper <= new_lower:
                # Old range is entirely before new range - deletions
                old_child = store.get_node(old_hash)
                yield from _yield_all_deletions(store, old_child)
                old_idx += 1
            elif new_upper is not None and old_lower is not None and new_upper <= old_lower:
                # New range is entirely before old range - additions
                new_child = store.get_node(new_hash)
                yield from _yield_all_additions(store, new_child)
                new_idx += 1
            else:
                # Ranges overlap - need to diff these children
                if old_hash == new_hash:
                    # Identical subtree - skip it!
                    old_idx += 1
                    new_idx += 1
                else:
                    # Different subtrees - recurse
                    old_child = store.get_node(old_hash)
                    new_child = store.get_node(new_hash)
                    yield from _diff_nodes(store, old_child, new_child)
                    old_idx += 1
                    new_idx += 1


def _get_child_ranges(node):
    """
    Get list of (child_hash, lower_bound, upper_bound) for each child in an internal node.

    Args:
        node: Internal node

    Returns:
        List of tuples (child_hash, lower_bound, upper_bound)
    """
    children = []
    for i, child_hash in enumerate(node.values):
        lower_bound = node.keys[i - 1] if i > 0 else None
        upper_bound = node.keys[i] if i < len(node.keys) else None
        children.append((child_hash, lower_bound, upper_bound))
    return children


def _diff_leaf_vs_internal(store: Store, old_entries: dict, new_node, is_old_leaf: bool) -> Iterator[DiffEvent]:
    """
    Diff a leaf node against an internal node.

    Args:
        store: Storage backend
        old_entries: Dictionary of old leaf entries
        new_node: New internal node
        is_old_leaf: Whether old is the leaf (vs new is the leaf)

    Yields:
        DiffEvent objects in key order
    """
    # Collect all entries from new internal node
    new_entries = {}
    for key, value in _collect_all_entries(store, new_node):
        new_entries[key] = value

    # Compare
    all_keys = sorted(set(old_entries.keys()) | set(new_entries.keys()))
    for key in all_keys:
        old_has = key in old_entries
        new_has = key in new_entries

        if old_has and new_has:
            if old_entries[key] != new_entries[key]:
                yield Modified(key, old_entries[key], new_entries[key])
        elif new_has:
            yield Added(key, new_entries[key])
        else:
            yield Deleted(key)


def _diff_internal_vs_leaf(store: Store, old_node, new_entries: dict) -> Iterator[DiffEvent]:
    """
    Diff an internal node against a leaf node.

    Args:
        store: Storage backend
        old_node: Old internal node
        new_entries: Dictionary of new leaf entries

    Yields:
        DiffEvent objects in key order
    """
    # Collect all entries from old internal node
    old_entries = {}
    for key, value in _collect_all_entries(store, old_node):
        old_entries[key] = value

    # Compare
    all_keys = sorted(set(old_entries.keys()) | set(new_entries.keys()))
    for key in all_keys:
        old_has = key in old_entries
        new_has = key in new_entries

        if old_has and new_has:
            if old_entries[key] != new_entries[key]:
                yield Modified(key, old_entries[key], new_entries[key])
        elif new_has:
            yield Added(key, new_entries[key])
        else:
            yield Deleted(key)


def _collect_all_entries(store: Store, node) -> Iterator[tuple]:
    """
    Recursively collect all entries from a node.

    Args:
        store: Storage backend
        node: Node to collect from

    Yields:
        Tuples of (key, value)
    """
    if node.is_leaf:
        for key, value in zip(node.keys, node.values):
            yield (key, value)
    else:
        for child_hash in node.values:
            child = store.get_node(child_hash)
            yield from _collect_all_entries(store, child)


def _yield_all_additions(store: Store, node) -> Iterator[Added]:
    """
    Yield Added events for all entries in a node.

    Args:
        store: Storage backend
        node: Node containing additions

    Yields:
        Added events for all entries
    """
    for key, value in _collect_all_entries(store, node):
        yield Added(key, value)


def _yield_all_deletions(store: Store, node) -> Iterator[Deleted]:
    """
    Yield Deleted events for all entries in a node.

    Args:
        store: Storage backend
        node: Node containing deletions

    Yields:
        Deleted events for all entries
    """
    for key, _ in _collect_all_entries(store, node):
        yield Deleted(key)
