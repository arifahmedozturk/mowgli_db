# Tasks

## Benchmark

- [x] **1.** Use big-endian `UINT64` key encoding in both bench and engine, then re-run
- [ ] **2.** Add cold-cache mode (drop page cache / dataset larger than RAM)
- [x] **3.** Run at 1M and 10M records (`bench/results/*_1M.json`, `*_10M.json`)
- [ ] **4.** Add multi-threaded mixed read/write benchmark
- [x] **5.** Report p50/p99/p999 latency
- [x] **6.** Report chain crossings and physical blocks per lookup, bytes per key, startup time, compaction time
- [x] **7.** Add key distributions: sequential u64, shared-prefix strings, variable-length strings
- [x] **8.** Add embedded B-tree baseline (LMDB / SQLite / RocksDB)
- [x] **9.** Measure WAL on vs off
- [ ] **10.** Profile the post-compaction lookup regression (940K/s → 138K/s)
- [x] **11.** Single script: Release build, N repetitions, record machine info, mean and variance
- [ ] **12.** Before/after numbers for subtree buckets
- [ ] **52.** `bench.cpp`: the `compact_lex` run copies the already-compacted bulk table (its comment says it starts from the original); copy the untouched `bulkcold` table instead

## Optimisations

- [ ] **13.** Subtree buckets: store a subtree as one sorted block once it fits; burst back into chains when it outgrows the block, merge back when it shrinks
- [ ] **14.** Avoid decompressing chains on every read after `COMPACT`
- [x] **15.** Zero-copy lookup: match on mapped bytes instead of decoding a `ChainData` per hop
- [ ] **16.** Remove global `grow_mutex_` from the `BufferPool::pin_*` fast path
- [ ] **17.** Patch parent pointer on chain promotion instead of relying on forwarding stubs
- [ ] **18.** Replace fixed 256 B `SLOT_CAP` with size classes
- [ ] **19.** Reuse dead packed slots and deleted heap space
- [x] **20.** Persist the Bloom filter instead of rebuilding it from every key on open (1M: open 1.45 s → 0.27 s)
- [ ] **21.** WAL group commit / async mode
- [ ] **22.** Size Bloom filter to key count
- [x] **23.** Replace `std::vector<bool>` prefixes in the range cursor with a byte buffer — done in #61: the cursor keeps a nibble buffer
- [x] **24.** Buffer socket reads in `recv_line`
- [ ] **25.** Finer-grained write locking than one `trie_latch_` per table
- [ ] **26.** Prefetch blocks for cold range scans
- [x] **50.** Investigate the `rebuild_counts` DFS on open (~240 ms at 1M, 85% of remaining startup): persist per-chain counts, or rebuild lazily on first write
- [ ] **51.** Investigate the packed-block scan in `DiskManager::open` (~45 ms at 1M): persist the pack directory instead of reading every block
- [ ] **53.** Compaction doubles the index (304 → 607 B/key at 1M and 10M, 910 after `compact_lex`): new slots are allocated while the old ones are live and freed blocks are never returned or truncated
- [ ] **54.** Range scans are 6–7% slower after `compact` / `compact_lex` at 1M (post-#14); investigate the layout for scans
- [ ] **55.** Cluster routing opens a new TCP connection per forwarded request (`cluster.cpp`); keep a pool of peer connections
- [ ] **60.** Extend zero-copy matching (#15) beyond `lookup`: the insert/remove descent and the range cursor still decode a `ChainData` per chain (only the chains they modify need decoding)
- [ ] **62.** Bulk load is 23–27% slower on base 16 than it was on base 2 (1M, all key sets): `insert_one_no_rebalance` copies each cached `ChainData`, now with a light-child vector per node, on every hop; walk without copying or build chains bottom-up from the sorted keys
- [ ] **63.** After compaction the base-16 index is 4–10% larger than base 2's (e.g. 607 → 671 B/key at 1M random keys) while it is 9–28% smaller before compaction; check how base-16 chains pack into slots during compaction
- [x] **58.** Investigated: caching chains on the read path (admit to the hot cache on a miss, weight-ranked) cut index blocks per lookup 4–8× at 1M (11.4 → 1.5 pre-compact) but made warm lookups 15–48% slower (pre-compact 241K → 189K/s, post-`compact_lex` 320K → 167K/s); a fill-only cache was also slower (180K/s). On a warm page cache a hot-cache hit (`ChainData` copy under `hot_mu_`) costs more than decoding from the mapped block, so blocks/lookup is not a speed proxy for warm reads. Not shipped; #15 (zero-copy lookup) is the better route, and the cache matters for cold I/O and write batching only

## Bugs

- [ ] **27.** Persist the heap header on every allocation, not just on close (crash overwrites live rows)
- [ ] **28.** Flush dirty hot-cache chains and `msync` before WAL commit, or replay committed entries (crash loses committed writes)
- [x] **29.** Persist or rebuild the Bloom filter on open (existing keys become unfindable after the first insert post-restart)
- [x] **30.** Flush the hot cache before `hot_.clear()` in `DiskTrie::bulk_insert`
- [ ] **31.** Handle a missing light child when an insert diverges at a record-only node (null-pointer crash on prefix keys)
- [x] **32.** Widen `split_bit` / `node_count` beyond `uint8_t` (keys over 32 bytes corrupt chains) — done in #61: split positions and node counts are 16-bit; tested with keys up to 300 bytes
- [x] **33.** Switch `UINT64` keys to big-endian (`RANGE` on numbers returns wrong rows; cluster routing too)
- [x] **34.** Take `table_latch_` in `Table::insert`, `remove`, `bulk_insert`, `lookup`
- [ ] **35.** Dedupe keys within a `BULK` batch
- [ ] **36.** Rebalance (flip) after `bulk_insert` into a non-empty trie
- [ ] **37.** Keep `ChainCounts::total` correct on remove and on duplicate insert
- [ ] **38.** Prune empty nodes and chains on remove
- [x] **39.** Persist free list entries beyond the ~1020 that fit in the header
- [x] **40.** Lift the 4 GB mmap file-size limit
- [x] **41.** Take `trie_latch_` before reading `bloom_` in `lookup`
- [x] **42.** Guard `hot_` reads in `chain_read_shared` with `hot_mu_`
- [x] **43.** Take `heap_mutex_` (or make mapping lock-free) in `HeapFile::read`
- [ ] **44.** Fix race on `repl_feed_threads` between the accept thread and shutdown
- [x] **45.** Cap frame length in `recv_msg` / `recv_line`
- [ ] **46.** Add idle timeout so connections can't hold all 64 workers
- [x] **47.** Update README/IMPROVEMENTS: range scan is no longer parallel; cluster hex boundaries after endianness fix
- [ ] **56.** Free-list crash safety: reusing a block from the free list doesn't update the on-disk header, so after a crash the persisted free list can name blocks in use (double allocation)
- [ ] **57.** Writer starvation: glibc `shared_mutex` favours readers, so steady reads can starve `Table` writers (20K inserts took 76 s against 2 looping readers in `test_concurrent_writes`); consider a writer-preferring latch


## Infrastructure

- [x] **48.** Add github-actions - run tests for each change in a pull request
- [ ] **49.** Refactor large bench files
- [x] **59.** Spike: should the trie move from base 2 (binary) to base 4 / 16 / 256? Deliverable: `trie-base-investigation.md` only — no engine changes.
  - Why revisit: base 2 was chosen so each branching node has exactly one light child, keeping the chains neighbouring any node to a minimum. Constraints have changed since (packed 256 B slots, mmap reads, compaction layout, chain decode cost per hop), so that may no longer be the deciding factor.
  - Compare per base: chain crossings and blocks per lookup, bits matched per step, node and chain size (up to k−1 light pointers per node), bytes per key, flip/rebalance frequency and cost, range-scan locality, and decode/compare CPU per hop (per-bit vs per-nibble/byte matching).
  - Check that the O(log n) light-edge bound still holds and what its constant becomes, and how each base fits the 8 KB block and packed-slot sizes.
  - Look across the key distributions in `bench` (`random_u64`, `seq_u64`, `prefix_str`, `varlen_str`) and at 1M/10M; estimates or a throwaway prototype are fine as long as the method is in the doc.
  - Conclude with the sweet spot (if any), the main drawbacks, a rough migration cost, and a go/no-go recommendation.
- [x] **61.** Implement the base-16 trie from #59 before other index work: nibble paths, up to 15 light children per node with a 16-bit child bitmap, 16-bit split positions; rework insert/flip/remove, cursor, compaction and tests; benchmark against base 2
