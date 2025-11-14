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
Storage backends for ProllyTree nodes.

Provides a Store protocol and multiple implementations:
- MemoryStore: In-memory storage using a dictionary
- FileSystemStore: Persistent storage using the filesystem
"""

from typing import Protocol, Optional
import json
import os


class Node:
    """Tree node - can be leaf or internal."""
    def __init__(self, is_leaf=True):
        self.is_leaf = is_leaf
        self.keys = []      # Separator keys (for internal) or actual keys (for leaves)
        self.values = []    # Child pointers (for internal) or actual values (for leaves)

    def __repr__(self):
        if self.is_leaf:
            return f"Leaf({list(zip(self.keys, self.values))})"
        else:
            return f"Internal(keys={self.keys}, children={len(self.values)})"


class Store(Protocol):
    """Protocol for node storage backends."""

    def put_node(self, node_hash: str, node: Node) -> None:
        """Store a node by its hash."""
        ...

    def get_node(self, node_hash: str) -> Optional[Node]:
        """Retrieve a node by its hash. Returns None if not found."""
        ...

    def count_nodes(self) -> int:
        """Return the total number of nodes in storage."""
        ...


class MemoryStore:
    """In-memory node storage using a dictionary."""

    def __init__(self):
        self.nodes = {}

    def put_node(self, node_hash: str, node: Node) -> None:
        """Store a node in memory."""
        self.nodes[node_hash] = node

    def get_node(self, node_hash: str) -> Optional[Node]:
        """Retrieve a node from memory."""
        return self.nodes.get(node_hash)

    def count_nodes(self) -> int:
        """Return the total number of nodes in storage."""
        return len(self.nodes)


class FileSystemStore:
    """File system-based node storage."""

    def __init__(self, base_path: str):
        """
        Initialize filesystem storage.

        Args:
            base_path: Directory to store nodes in
        """
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    def _node_path(self, node_hash: str) -> str:
        """Get the file path for a node hash."""
        # Use first 2 chars as subdirectory for better filesystem performance
        subdir = node_hash[:2]
        dir_path = os.path.join(self.base_path, subdir)
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, node_hash)

    def _serialize_node(self, node: Node) -> str:
        """Serialize a node to JSON."""
        return json.dumps({
            'is_leaf': node.is_leaf,
            'keys': node.keys,
            'values': node.values
        })

    def _deserialize_node(self, data: str) -> Node:
        """Deserialize a node from JSON."""
        obj = json.loads(data)
        node = Node(is_leaf=obj['is_leaf'])
        node.keys = obj['keys']
        node.values = obj['values']
        return node

    def put_node(self, node_hash: str, node: Node) -> None:
        """Store a node to filesystem."""
        path = self._node_path(node_hash)
        with open(path, 'w') as f:
            f.write(self._serialize_node(node))

    def get_node(self, node_hash: str) -> Optional[Node]:
        """Retrieve a node from filesystem."""
        path = self._node_path(node_hash)
        if not os.path.exists(path):
            return None
        with open(path, 'r') as f:
            return self._deserialize_node(f.read())

    def count_nodes(self) -> int:
        """Return the total number of nodes in storage."""
        count = 0
        for subdir in os.listdir(self.base_path):
            subdir_path = os.path.join(self.base_path, subdir)
            if os.path.isdir(subdir_path):
                count += len([f for f in os.listdir(subdir_path) if os.path.isfile(os.path.join(subdir_path, f))])
        return count


def create_store_from_spec(spec: str) -> Store:
    """
    Create a store from a specification string.

    Args:
        spec: Store specification, one of:
            - ':memory:' - in-memory storage
            - 'file:///path/to/dir' - filesystem storage
            - 's3://bucket-name' - S3 storage (not yet implemented)

    Returns:
        Store instance
    """
    if spec == ':memory:':
        return MemoryStore()
    elif spec.startswith('file://'):
        # Remove 'file://' prefix
        path = spec[7:]
        return FileSystemStore(path)
    elif spec.startswith('s3://'):
        raise NotImplementedError("S3 storage not yet implemented")
    else:
        raise ValueError(f"Invalid store spec: {spec}")
