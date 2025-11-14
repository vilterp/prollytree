# ProllyTree Incremental Batch Insert Prototype

This directory contains a simplified Python prototype to understand and verify the incremental batch insert algorithm before implementing it in Rust.

## Key Concepts

The prototype demonstrates **DoltDB-style incremental batch insert** with node reuse:

1. **Range-based partitioning**: Mutations are partitioned to child subtrees based on separator keys
2. **Selective traversal**: Only subtrees affected by mutations are traversed and rebuilt
3. **Subtree reuse**: Unchanged subtrees are reused by keeping their hash pointers (no traversal!)

## Simplifications

- Fixed-size nodes (max 4 keys) instead of content-defined splitting
- No rolling hash (just split when full)
- Simplified storage (in-memory dict)

## Running the Tests

```bash
python3 prolly_tree.py
```

## Expected Results

- **Test 1**: Empty tree insert - creates 3 nodes (2 leaves + 1 parent), 0 reused
- **Test 2**: Interleaved keys - affects both children, 0 subtrees reused
- **Test 3**: Keys in one range - affects only right child, **1 subtree reused** ✓

## Key Insight

When inserting keys 13-16 (Test 3), the left subtree (keys 1-9) is **completely unchanged**. The algorithm:
1. Detects no mutations fall in left child's range
2. **Reuses the existing child hash pointer directly**
3. Never reads or traverses the left subtree
4. Only rebuilds the affected right subtree

This is O(M log N) instead of O(N) because we skip entire unchanged subtrees!

## Next Step

Port this logic to Rust in `src/node.rs`, ensuring:
- Properly handle content-defined splitting (rolling hash)
- Handle case where rebuilt children split into multiple nodes
- Update parent separator keys when child structure changes
