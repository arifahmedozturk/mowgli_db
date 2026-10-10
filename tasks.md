# Tasks

## Benchmark

- [x] **1.** Use big-endian `UINT64` key encoding in both bench and engine, then re-run
- [ ] **2.** Add cold-cache mode (drop page cache / dataset larger than RAM)
- [ ] **3.** Run at 1M and 10M records
- [ ] **4.** Add multi-threaded mixed read/write benchmark
- [x] **5.** Report p50/p99/p999 latency
- [x] **6.** Report chain crossings and physical blocks per lookup, bytes per key, startup time, compaction time
- [x] **7.** Add key distributions: sequential u64, shared-prefix strings, variable-length strings
- [ ] **8.** Add embedded B-tree baseline (LMDB / SQLite / RocksDB)
- [x] **9.** Measure WAL on vs off
- [ ] **10.** Profile the post-compaction lookup regression (940K/s → 138K/s)
- [x] **11.** Single script: Release build, N repetitions, record machine info, mean and variance
- [ ] **12.** Before/after numbers for subtree buckets

## Optimisations

- [ ] **13.** Subtree buckets: store a subtree as one sorted block once it fits; burst back into chains when it outgrows the block, merge back when it shrinks
- [ ] **14.** Avoid decompressing chains on every read after `COMPACT`
- [ ] **15.** Zero-copy lookup: match on mapped bytes instead of decoding a `ChainData` per hop
- [ ] **16.** Remove global `grow_mutex_` from the `BufferPool::pin_*` fast path
- [ ] **17.** Patch parent pointer on chain promotion instead of relying on forwarding stubs
- [ ] **18.** Replace fixed 256 B `SLOT_CAP` with size classes
- [ ] **19.** Reuse dead packed slots and deleted heap space
- [ ] **20.** Persist pack directory and counts instead of scanning the whole file on open
- [ ] **21.** WAL group commit / async mode
- [ ] **22.** Size Bloom filter to key count
- [ ] **23.** Replace `std::vector<bool>` prefixes in the range cursor with a byte buffer
- [ ] **24.** Buffer socket reads in `recv_line`
- [ ] **25.** Finer-grained write locking than one `trie_latch_` per table
- [ ] **26.** Prefetch blocks for cold range scans

## Bugs

- [ ] **27.** Persist the heap header on every allocation, not just on close (crash overwrites live rows)
- [ ] **28.** Flush dirty hot-cache chains and `msync` before WAL commit, or replay committed entries (crash loses committed writes)
- [x] **29.** Persist or rebuild the Bloom filter on open (existing keys become unfindable after the first insert post-restart)
- [ ] **30.** Flush the hot cache before `hot_.clear()` in `DiskTrie::bulk_insert`
- [ ] **31.** Handle a missing light child when an insert diverges at a record-only node (null-pointer crash on prefix keys)
- [ ] **32.** Widen `split_bit` / `node_count` beyond `uint8_t` (keys over 32 bytes corrupt chains)
- [x] **33.** Switch `UINT64` keys to big-endian (`RANGE` on numbers returns wrong rows; cluster routing too)
- [ ] **34.** Take `table_latch_` in `Table::insert`, `remove`, `bulk_insert`, `lookup`
- [ ] **35.** Dedupe keys within a `BULK` batch
- [ ] **36.** Rebalance (flip) after `bulk_insert` into a non-empty trie
- [ ] **37.** Keep `ChainCounts::total` correct on remove and on duplicate insert
- [ ] **38.** Prune empty nodes and chains on remove
- [ ] **39.** Persist free list entries beyond the ~1020 that fit in the header
- [ ] **40.** Lift the 4 GB mmap file-size limit
- [ ] **41.** Take `trie_latch_` before reading `bloom_` in `lookup`
- [x] **42.** Guard `hot_` reads in `chain_read_shared` with `hot_mu_`
- [ ] **43.** Take `heap_mutex_` (or make mapping lock-free) in `HeapFile::read`
- [ ] **44.** Fix race on `repl_feed_threads` between the accept thread and shutdown
- [x] **45.** Cap frame length in `recv_msg` / `recv_line`
- [ ] **46.** Add idle timeout so connections can't hold all 64 workers
- [x] **47.** Update README/IMPROVEMENTS: range scan is no longer parallel; cluster hex boundaries after endianness fix


## Infrastructure

- [x] **48.** Add github-actions - run tests for each change in a pull request
- [ ] **49.** Refactor large bench files