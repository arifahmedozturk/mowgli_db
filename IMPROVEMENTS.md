# Implementation History

This document traces the evolution of heavy-trie from a naive prototype to its current form. Each section corresponds to one significant improvement, with before/after numbers measured on a single thread (WSL2, Release build, UINT64 primary key + 64-byte VARCHAR value, 100K records, mean of 10 runs).

---

## Baseline — naive pread/pwrite buffer pool

Each block read/write was a `pread`/`pwrite` syscall against a small LRU buffer pool. No free list: blocks orphaned by flip operations were never reclaimed.

| Operation | Throughput | Notes |
|---|---|---|
| INSERT | 10.97 K/s | |
| LOOKUP | 71.49 K/s | avg 8.75 chain crossings per key |
| RANGE | 3.75 scans/s | 100 scans × ~100 keys each |
| Allocated blocks | 149,978 | ~50K orphaned by flips |

---

## Improvement 1 — mmap I/O + block recycling

Replaced the `pread`/`pwrite` LRU buffer pool with an mmap-backed pool that reserves 4 GB of virtual address space at open and extends the file with `ftruncate` + `MAP_FIXED` on demand. The base address never moves so `pin_shared` pointers are stable for the lifetime of the process. Range scan now decodes chain data directly from the mapped pointer — no per-block 8 KB copy. Orphaned chain blocks from flip operations go onto an in-memory free list (persisted in the header block) and are reused by `alloc_block`.

| Operation | Before | After | Δ |
|---|---|---|---|
| INSERT | 10.97 K/s | 13.67 K/s | +25% |
| LOOKUP | 71.49 K/s | 184.23 K/s | +158% |
| RANGE | 3.75 /s | 13.27 /s | +254% |
| Allocated blocks | 149,978 | 100,001 | −50K recycled |

---

## Improvement 2 — lazy header flushes, batched allocation, hot chain cache, skip redundant weight writes

Previously each insert flushed the header block 2–3 times and re-encoded every ancestor block on the weight-update pass even when the on-disk value was already correct.

- `set_key_count` and `free_block` are lazy — in-memory only, flushed by the destructor. Losing these on crash wastes a block or leaves `COUNT` off by one; neither corrupts the trie.
- `alloc_block` pre-commits a ceiling of 64 IDs per flush instead of flushing on every allocation.
- 1000-entry write-back LRU cache for decoded `ChainData`. Writes increment a dirty counter; flush to mmap when dirty hits 10 or the entry is evicted.
- Weight update pass skips writing when `floor(log2(new_count))` equals the stored value. At depth d the log2 value changes only once per ~2^d inserts, so upper-level blocks almost never need rewriting.

| Operation | Before | After | Δ |
|---|---|---|---|
| INSERT | 13.67 K/s | 15.19 K/s | +11% |
| LOOKUP | 184.23 K/s | 264.71 K/s | +44% |
| RANGE | 13.27 /s | 13.47 /s | +2% |

---

## Improvement 3 — heap file mmap + pre-grow both files

Every insert was making 3 `pread`/`pwrite` syscalls on the heap file; every lookup made 1. Both files also called `ftruncate` + `MAP_FIXED` on every single new block.

- Heap file converted to mmap with the same 4 GB + `MAP_FIXED` pattern. All page reads/writes are pointer dereferences.
- Both files pre-grow in 8 MB chunks (1024 blocks) so `ftruncate` fires at most once per 1024 allocations.

| Operation | Before | After | Δ |
|---|---|---|---|
| INSERT | 15.19 K/s | 26.39 K/s | +74% |
| LOOKUP | 264.71 K/s | 445.44 K/s | +68% |
| RANGE | 13.47 /s | 13.78 /s | +2% |

---

## Improvement 4 — parallel range scan

Range scan was purely sequential. Light subtrees are independent of the heavy path and of each other, so they can run concurrently.

- Each light child encountered during traversal is dispatched as a `std::async` task; the heavy path continues inline.
- Spawning is gated on `light_child_weight ≥ 9` (`floor(log2(count)) ≥ 9`, meaning ≥ ~512 keys in the subtree). Smaller subtrees run inline — thread overhead exceeds the benefit.
- Results return as sorted vectors and are merged with `std::merge` at the join point.

| Operation | Before | After | Δ |
|---|---|---|---|
| INSERT | 26.39 K/s | 25.58 K/s | — (variance) |
| LOOKUP | 445.44 K/s | 476.61 K/s | +7% |
| RANGE | 13.78 /s | 17.57 /s | +27% |

---

## Improvement 5 — bulk insert

Single `insert()` does O(log n) read-modify-write weight updates per key plus flip checks at every ancestor. For large batch loads that work is repeated thousands of times on the same ancestor blocks.

- `BULK` inserts all keys with weight writes suppressed and flips disabled.
- After the load, a single DFS (`rebuild_counts`) computes exact subtree sizes, followed by one pass that writes correct weights to every block that needs updating — O(n) total instead of O(n log n).

| Mode | Throughput | Δ |
|---|---|---|
| Single insert | ~25 K/s | — |
| Bulk insert | ~41 K/s | +65% |

---

## Improvement 6 — multi-chain block packing

**The problem:** each chain block used ~26 bytes out of 8192 (0.3% utilization for a leaf chain). Range scans paid one block read per chain touched, regardless of chain size.

**The fix:** pack multiple chains into one 8 KB block with a directory header.

Block layout:
```
[0..3]   magic (PACK_BLOCK_MAGIC)
[4..5]   num_slots
[6..1025] directory: {offset(2), capacity(2)} × 255 entries
[1026..8191] chain data — up to 27 slots of 256 bytes each
```

Each slot reserves 256 bytes (= `PACK_THRESHOLD`). Chains smaller than 256 bytes are stored packed; chains that grow beyond 256 bytes are promoted to a dedicated block via an in-place forwarding stub so existing parent references stay valid without a tree-wide update. On startup, `rebuild_packed_state` scans allocated blocks to reconstruct the in-memory directory.

Chain address encoding: `addr = (slot << 56) | phys_block_id` — the top 8 bits carry the slot index (0 = dedicated block, 1–255 = packed slot). `NULL_BLOCK` (UINT64_MAX) remains the null sentinel.

| Metric | Before | After | Δ |
|---|---|---|---|
| Bulk insert | 76 K/s | **310 K/s** | +4.1× |
| Single insert | 47 K/s | **93 K/s** | +2.0× |
| Lookup | 234 K/s | **469 K/s** | +2.0× |
| Range scan | 13.8 /s | **18.0 /s** | +1.3× |
| Physical blocks (100K records) | ~100,000 | **~5,700** | **17× fewer** |

The range scan gain is modest at 100K records because the full dataset fits in the Linux page cache after the first pass. At cold-cache scale (data larger than RAM) the 17× block reduction translates directly to 17× fewer disk reads for range scans.

---

## Improvement 7 — branch pruning in range scan

**The problem:** the range scan DFS always descended both children at every branch, checking `key >= lo && key <= hi` only at the leaf. On random uint64 keys, most branches lead entirely outside the query range — but the old code traversed them anyway.

**The fix:** before descending any child, check whether every key in that subtree is provably outside [lo, hi]:
- If `prefix + 1...1` (the subtree's max possible key) is already below `lo` → skip.
- If `prefix + 0...0` (the subtree's min possible key) already exceeds `hi` → skip.

Both checks are a single prefix comparison against lo/hi, done bit by bit until a difference is found. The pruning fires at trie entry, at each branch before descending the heavy child inline, and before spawning async tasks for light children.

| Operation | Before | After | Δ |
|---|---|---|---|
| Range (~100 keys, 100 scans) | 18.0 /s | **4,206 /s** | **+234×** |
| Range narrow (lo==hi, 500 scans) | — | **7,595 /s** | new |

The 234× gain on 100-key ranges shows how much of the trie was being traversed needlessly. Random uint64 keys diverge from lo/hi after only a few bits, so pruning fires very early and eliminates nearly the entire tree outside the target interval.

---

## Improvement 8 — compaction (DFS pre-order repack)

After many single inserts the trie's chain blocks are scattered across the file in allocation order, not traversal order. A `COMPACT` command does a DFS pre-order rewrite: each chain's data is written to a fresh block positioned in the order it would be visited during a lookup or range scan. Parent references are updated in-place via forwarding stubs during the rewrite, then a second pass patches all stubs to point directly at the new locations.

Effect at 100K records (bulk-loaded table):

| Operation | Before compact | After compact | Δ |
|---|---|---|---|
| Lookup | ~470 K/s | ~940 K/s | **+2×** |
| Range scan | ~4,200 /s | ~3,270 /s | −22% |

The lookup gain comes from DFS pre-order placing parent and heavy-child blocks adjacently — a lookup traverses them in sequence, hitting warm cache lines. The range regression is the flip side: DFS pre-order co-locates trie-traversal neighbors, not lex-order neighbors. Adjacent keys in lex order live in different subtrees, so range scan now touches cold blocks instead of the warm sequential run it had before compaction. A lex-order compaction strategy would fix this but is not yet implemented.
