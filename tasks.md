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
- [x] **12.** Before/after numbers for subtree buckets (`bench/results/*1M-base16.json` vs `*1M-buckets.json`: 1M random keys, lookups 322K → 662K/s, range scans 3.5–6.7×, index 369 → 36 B/key, startup 54 → 3.5 ms)
- [ ] **52.** `bench.cpp`: the `compact_lex` run copies the already-compacted bulk table (its comment says it starts from the original); copy the untouched `bulkcold` table instead

## Optimisations

- [x] **13.** Subtree buckets: store a subtree as one sorted block once it fits; burst back into chains when it outgrows the block, merge back when it shrinks
- [ ] **14.** Avoid decompressing chains on every read after `COMPACT`
- [x] **15.** Zero-copy lookup: match on mapped bytes instead of decoding a `ChainData` per hop
- [ ] **16.** Remove global `grow_mutex_` from the `BufferPool::pin_*` fast path
- [ ] **17.** Patch parent pointer on chain promotion instead of relying on forwarding stubs
- [x] **18.** Replace fixed 256 B `SLOT_CAP` with size classes (64 B–4 KB power-of-two slots; 1M string keys: index −50%, compacted −67–76%; random u64: +12% from dead slots left by moves, see #19)
- [x] **19.** Reuse dead packed slots and deleted heap space — freed packed slots keep their region and are reused best-fit; heap pages compact deleted rows in place and plain appends refill pages that deletes emptied. 1M keys after removing half and re-inserting as many: heap 210–285 → 165–213 B/key (flat vs before the churn); index unchanged (it did not grow here before either)
- [x] **20.** Persist the Bloom filter instead of rebuilding it from every key on open (1M: open 1.45 s → 0.27 s)
- [ ] **21.** WAL group commit / async mode
- [ ] **22.** Size Bloom filter to key count
- [x] **23.** Replace `std::vector<bool>` prefixes in the range cursor with a byte buffer — done in #61: the cursor keeps a nibble buffer
- [x] **24.** Buffer socket reads in `recv_line`
- [ ] **25.** Finer-grained write locking than one `trie_latch_` per table
- [ ] **26.** Prefetch blocks for cold range scans
- [x] **50.** Investigate the `rebuild_counts` DFS on open (~240 ms at 1M, 85% of remaining startup): persist per-chain counts, or rebuild lazily on first write
- [ ] **51.** Investigate the packed-block scan in `DiskManager::open` (~45 ms at 1M): persist the pack directory instead of reading every block
- [x] **53.** Compaction doubles the index (304 → 607 B/key at 1M and 10M, 910 after `compact_lex`): new slots are allocated while the old ones are live and freed blocks are never returned or truncated — done: pointers are written with final block ids, and after the old slots are freed the new layout slides down to the front of the file (same order) and the file is truncated. 1M (3 reps): index after `compact` 67.7 → 33.6 B/key random_u64, 87.7 → 43.7 prefix_str, 88.5 → 44.0 varlen_str (same after `compact_lex`), i.e. slightly below the pre-compaction size; compaction takes longer (83 → 124 ms, `compact_lex` 46 → 140 ms) since each run now allocates fresh pages past the end instead of reusing the freed half. Not crash safe: blocks are overwritten in place during the slide
- [ ] **54.** Range scans are 6–7% slower after `compact` / `compact_lex` at 1M (post-#14); investigate the layout for scans
- [ ] **55.** Cluster routing opens a new TCP connection per forwarded request (`cluster.cpp`); keep a pool of peer connections
- [x] **60.** Extend zero-copy matching (#15) beyond `lookup`: the insert/remove descent and the range cursor still decode a `ChainData` per chain (only the chains they modify need decoding) — done for insert/remove: chains the key only passes through are matched in place (hot cache or mapped bytes) with the frames recorded from the match, and `adjust_weights` works from the in-memory counts, reading a chain only when its stored weight changes or a flip/merge is due. At 1M it is even with master (the passed chains sit in the hot cache anyway); at 10M (single run) inserts 82K → 86K/s, p50 8.2 → 7.8 µs, removes 56K → 87K/s. The model test now checks stored weights and balance against the structure after every round. The range cursor part moved to #64
- [x] **62.** Bulk load is 23–27% slower on base 16 than it was on base 2 (1M, all key sets): `insert_one_no_rebalance` copies each cached `ChainData`, now with a light-child vector per node, on every hop; walk without copying or build chains bottom-up from the sorted keys — resolved by #13: bulk loads into an empty trie are built bottom-up (1M: 155K → 572K/s)
- [ ] **63.** After compaction the base-16 index is 4–10% larger than base 2's (e.g. 607 → 671 B/key at 1M random keys) while it is 9–28% smaller before compaction; check how base-16 chains pack into slots during compaction
- [ ] **64.** Range scans are still ~10× behind LMDB after #13 (1M random keys: 2.2K vs 21.5K scans/s); profile the cursor (per-key prefix copies, `nibbles_to_key`) and `Table::range` row decoding; also from #60: the cursor still decodes a `ChainData` per chain it descends through
- [x] **65.** Keep rows near their key neighbours in the heap — done with B-tree-style heap page splits: every heap page holds a contiguous key range; a key inside a full page splits it at the key (larger keys move to a new page and are repointed in the trie), keys at a page edge go to an adjacent page with room or a new page. 1M random inserts: range scans 2.1K → 3.7K/s (bulk-loaded speed), heap 79 → 158 B/key, inserts −27%
- [ ] **66.** Look for improvements to range scans (open-ended)
- [x] **67.** Heap pages after #65 run about half full (158 vs 79 B/key at 1M): split at the page midpoint instead of at the inserted key, or shift rows into an adjacent page with room before splitting, and merge sparse neighbouring pages on delete — done: shift top rows into the next page when it has ≥1/8 free, else B*-tree 2→3 split, else median split; merge sparse pages into their predecessor on delete. 1M (3 reps): heap 158 → 100 B/key random_u64, 208 → 133 prefix_str, 211 → 134 varlen_str (also after churn); range ±4%; insert −6 to −20%, p99 59 → 88 µs (random_u64), 90 → 171 (varlen_str) — see #68
- [x] **68.** Make #65 page splits cheaper (insert p99 17 → 57 µs at 1M): the moved keys are contiguous, so repoint them in one ordered walk of the trie instead of one `set_record` descent per key; after #67 the p99 is 88 µs (random_u64) / 171 µs (varlen_str) since shifts and 2→3 splits move rows more often — done: profiling showed only ~2% of inserts split/shift but they are the whole p99, and ~80% of a split was heap work, not repointing. Now: `DiskTrie::set_records` repoints moved keys with one descent and one write per bucket; split planning sorts key views into the pages (8-byte big-endian head, then the key) instead of copying every row; moved rows are copied once; new heap file space is prefaulted with `MADV_POPULATE_WRITE` when the file grows (a split's new page paid ~20 µs of first-touch faults). 1M (3 reps): insert p99 87 → 37 µs random_u64, 104 → 41 prefix_str, 175 → 66 varlen_str; p999 −37 to −53%; inserts/s +17 to +28%; heap size unchanged
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
- [x] **35.** Dedupe keys within a `BULK` batch — done: stable sort, then skip a key equal to the previous one, so the first row given wins (as with repeated inserts); repeats no longer leak heap rows or inflate the key count
- [ ] **36.** Rebalance (flip) after `bulk_insert` into a non-empty trie
- [ ] **37.** Keep `ChainCounts::total` correct on remove and on duplicate insert
- [ ] **38.** Prune empty nodes and chains on remove
- [x] **39.** Persist free list entries beyond the ~1020 that fit in the header
- [x] **40.** Lift the 4 GB mmap file-size limit
- [x] **41.** Take `trie_latch_` before reading `bloom_` in `lookup`
- [x] **42.** Guard `hot_` reads in `chain_read_shared` with `hot_mu_`
- [x] **43.** Take `heap_mutex_` (or make mapping lock-free) in `HeapFile::read`
- [x] **44.** Fix race on `repl_feed_threads` between the accept thread and shutdown — done: new `ReplServer` owns the accept thread; only it touches the feed threads (reaping finished ones) and `stop()` joins them all before the `ReplLog` is destroyed; 2 s handshake timeout so a silent client cannot block shutdown
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
