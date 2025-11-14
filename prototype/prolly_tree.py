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
Compatibility module - re-exports from tree and store modules.

This module exists for backward compatibility with existing code.
New code should import directly from tree and store modules.
"""

# Re-export everything from store and tree
from store import Node, Store, MemoryStore, FileSystemStore, CachedFSStore, create_store_from_spec
from tree import ProllyTree

__all__ = [
    'Node',
    'Store',
    'MemoryStore',
    'FileSystemStore',
    'CachedFSStore',
    'create_store_from_spec',
    'ProllyTree',
]
