# heavy-trie

heavy-trie is a custom disk-backed database engine that applies **heavy path decomposition** to a binary trie. The structural trick: any root-to-leaf path crosses at most O(log n) *heavy chains*, and each chain lives in exactly one 8 KB disk block, so a point lookup costs O(log n) block reads — the same guarantee as a B-tree — while prefix queries and lexicographic range scans are structural properties of the trie rather than bolt-on features. The engine ships its own mmap storage layer, a write-back hot-chain cache, parallel range scan, bulk-load path, and a multi-chain block packing scheme that stores up to 27 small chains per 8 KB block, cutting physical block count by 17× and making cold-cache range scans competitive with a B-tree.

---

## Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

Requirements: **g++ ≥ 11** (C++20), **CMake ≥ 3.20**.  
On Ubuntu/Debian: `sudo apt-get install build-essential cmake`

## Run

```bash
./build/mql              # data stored in ./data/
./build/mql ./mydb       # custom data directory
```

Data persists across restarts — tables are stored as `.trie`, `.heap`, and `.schema` files in the data directory.

## Benchmark

```bash
./build/bench            # 100K records, ./bench_data/
./build/bench 500000     # 500K records
```

---

## Example session

```
TABLE users(id string PRIMARY KEY, age number, name string)

NEW(users, 'alice', 30, 'Alice Smith')
NEW(users, 'bob',   25, 'Bob Jones')
NEW(users, 'carol', 28, 'Carol White')

QUERY(users, 'alice')
→ alice | 30 | Alice Smith  [1 chains]

RANGE(users, 'alice', 'carol')
→ alice | 30 | Alice Smith
→ bob   | 25 | Bob Jones
→ carol | 28 | Carol White
(3 rows)

UPDATE(users, 'alice', 31, 'Alice Smith')
DELETE(users, 'bob')
COUNT(users)
→ 2

BULK users ('dave', 22, 'Dave Brown') ('eve', 35, 'Eve Davis')

IN(users, 'alice', 'eve')
→ alice | 31 | Alice Smith
→ eve   | 35 | Eve Davis

CHAINS(users)
→ 3 active, 3 allocated

exit
```

### Full MQL reference

| Command | Syntax |
|---|---|
| Create table | `TABLE name(col type [PRIMARY KEY], ...)` |
| Insert row | `NEW(table, val, ...)` |
| Update row | `UPDATE(table, val, ...)` |
| Bulk insert | `BULK table (val, ...) (val, ...) ...` |
| Point query | `QUERY(table, pk)` |
| Range scan | `RANGE(table, lo, hi)` |
| IN lookup | `IN(table, k1, k2, ...)` |
| Delete | `DELETE(table, pk)` |
| Count | `COUNT(table)` |
| Chain stats | `CHAINS(table)` |

Types: `string`, `number` (uint64). Exactly one column must be `PRIMARY KEY`.

---

## Benchmarks

Measured on a single thread (WSL2, Release build, UINT64 primary key + 64-byte VARCHAR value).  
Each row in the averaged tables is the mean of 10 independent runs at 100K records.

### Baseline (naive pread/pwrite buffer pool)

| Operation | Throughput | Notes |
|---|---|---|
| INSERT | 10.97 K/s | |
| LOOKUP | 71.49 K/s | avg 8.75 chain crossings per key |
| RANGE | 3.75 scans/s | 100 scans × ~100 keys each |
| Allocated blocks | 149,978 | ~50K orphaned by flips |

---

### Improvement 1 — mmap I/O + block recycling

Replaced the `pread`/`pwrite` LRU buffer pool with an mmap-backed pool that reserves 4 GB of virtual address space at open and extends the file with `ftruncate` + `MAP_FIXED` on demand. The base address never moves so `pin_shared` pointers are stable for the lifetime of the process. Range scan now decodes chain data directly from the mapped pointer — no per-block 8 KB copy. Orphaned chain blocks from flip operations go onto an in-memory free list (persisted in the header block) and are reused by `alloc_block`.

| Operation | Before | After | Δ |
|---|---|---|---|
| INSERT | 10.97 K/s | 13.67 K/s | +25% |
| LOOKUP | 71.49 K/s | 184.23 K/s | +158% |
| RANGE | 3.75 /s | 13.27 /s | +254% |
| Allocated blocks | 149,978 | 100,001 | −50K recycled |

---

### Improvement 2 — lazy header flushes, batched allocation, hot chain cache, skip redundant weight writes

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

### Improvement 3 — heap file mmap + pre-grow both files

Every insert was making 3 `pread`/`pwrite` syscalls on the heap file; every lookup made 1. Both files also called `ftruncate` + `MAP_FIXED` on every single new block.

- Heap file converted to mmap with the same 4 GB + `MAP_FIXED` pattern. All page reads/writes are pointer dereferences.
- Both files pre-grow in 8 MB chunks (1024 blocks) so `ftruncate` fires at most once per 1024 allocations.

| Operation | Before | After | Δ |
|---|---|---|---|
| INSERT | 15.19 K/s | 26.39 K/s | +74% |
| LOOKUP | 264.71 K/s | 445.44 K/s | +68% |
| RANGE | 13.47 /s | 13.78 /s | +2% |

---

### Improvement 4 — parallel range scan

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

### Improvement 5 — bulk insert

Single `insert()` does O(log n) read-modify-write weight updates per key plus flip checks at every ancestor. For large batch loads that work is repeated thousands of times on the same ancestor blocks.

- `BULK` inserts all keys with weight writes suppressed and flips disabled.
- After the load, a single DFS (`rebuild_counts`) computes exact subtree sizes, followed by one pass that writes correct weights to every block that needs updating — O(n) total instead of O(n log n).

| Mode | Throughput | Δ |
|---|---|---|
| Single insert | ~25 K/s | — |
| Bulk insert | ~41 K/s | +65% |

---

### Improvement 6 — multi-chain block packing

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

## Theory — heavy path decomposition on a trie

### Binary trie basics

A binary trie stores keys bit by bit. Each internal node splits on one bit: keys with a 0 at that position go left, keys with a 1 go right. A root-to-leaf traversal for an n-bit key visits exactly n nodes. Storing each node in its own disk block would cost n I/Os per lookup — too expensive.

### Path compression (Patricia / radix trie)

Runs of bits with no branching are compressed into a single edge label. A chain of k non-branching bits costs one node and one I/O instead of k. This brings the worst-case depth down to the number of distinct branch points, which is at most the number of keys − 1.

### The heavy path trick

Assign every internal node a *weight* = number of keys in its subtree. At each node, the child with the larger weight is the *heavy* child; the other is the *light* child. The path from the root that always follows the heavy child is the *heavy path*.

**Key property:** on any root-to-leaf path, a *light edge* (crossing to a light child) at least halves the remaining subtree weight. Therefore any path crosses at most O(log n) light edges — and at most O(log n) heavy chains.

### One chain = one disk block

Each maximal consecutive run of heavy edges (a heavy chain) is encoded into a single 8 KB block along with the path bits and the addresses of any light children that branch off it. A lookup reads one block per chain crossing, so the total I/O is O(log n) — the same as a B-tree.

### Rebalancing (flip)

When an insert causes a light subtree to outgrow its heavy sibling, the two are swapped: the old light child becomes the new heavy path, and the displaced heavy tail becomes a new light-child block. This *flip* restores the heavy-path invariant in O(1) extra I/Os and is triggered at most O(log n) times per insert, keeping the tree balanced.

### Why this matters for range scans

In a B-tree, range scans are fast because leaf pages are linked and sorted — a scan reads consecutive pages. In a heavy-trie, the lexicographic order is encoded in the trie structure itself: left subtrees (bit=0) always precede right subtrees (bit=1). An in-order traversal naturally produces sorted output. With block packing, small chains that would otherwise occupy individual 8 KB blocks are co-located in shared blocks, giving the same page-sequential access pattern a B-tree enjoys.

---

## Project structure

```
heavy-trie/
├── main.cpp              REPL entry point
├── bench/
│   └── bench.cpp         Standalone benchmark (BULK/INSERT/LOOKUP/RANGE)
├── index/
│   ├── chain.h/cpp       Chain encoding/decoding, path matching, slice I/O
│   └── disk_trie.h/cpp   Trie insert/lookup/delete/range + rebalancing + hot cache
├── storage/
│   ├── buffer_pool.h/cpp mmap-backed pool with stripe latches
│   ├── disk_manager.h/cpp Block allocation, free list, packed-block manager
│   └── heap.h/cpp        Slotted-page record storage (mmap-backed)
├── catalog/
│   └── table.h/cpp       Schema + trie + heap wired together
├── mql/
│   ├── lexer.h/cpp       Tokenizer
│   └── engine.h/cpp      Parser + executor
└── tests/                One test file per component
```

## Run tests

```bash
cd build && ctest --output-on-failure
```
