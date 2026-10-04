# Tasks

## Benchmark

- [ ] Use big-endian `UINT64` key encoding in both bench and engine, then re-run
- [ ] Add cold-cache mode (drop page cache / dataset larger than RAM)
- [ ] Run at 1M and 10M records
- [ ] Add multi-threaded mixed read/write benchmark
- [ ] Report p50/p99/p999 latency
- [ ] Report chain crossings and physical blocks per lookup, bytes per key, startup time, compaction time
- [ ] Add key distributions: sequential u64, shared-prefix strings, variable-length strings
- [ ] Add embedded B-tree baseline (LMDB / SQLite / RocksDB)
- [ ] Measure WAL on vs off
- [ ] Profile the post-compaction lookup regression (940K/s → 138K/s)
- [ ] Single script: Release build, N repetitions, record machine info, mean and variance
- [ ] Before/after numbers for subtree buckets

## Optimisations

- [ ] Subtree buckets: store a subtree as one sorted block once it fits; burst back into chains when it outgrows the block, merge back when it shrinks
- [ ] Avoid decompressing chains on every read after `COMPACT`
- [ ] Zero-copy lookup: match on mapped bytes instead of decoding a `ChainData` per hop
- [ ] Remove global `grow_mutex_` from the `BufferPool::pin_*` fast path
- [ ] Patch parent pointer on chain promotion instead of relying on forwarding stubs
- [ ] Replace fixed 256 B `SLOT_CAP` with size classes
- [ ] Reuse dead packed slots and deleted heap space
- [ ] Persist pack directory and counts instead of scanning the whole file on open
- [ ] WAL group commit / async mode
- [ ] Size Bloom filter to key count
- [ ] Replace `std::vector<bool>` prefixes in the range cursor with a byte buffer
- [ ] Buffer socket reads in `recv_line`
- [ ] Finer-grained write locking than one `trie_latch_` per table
- [ ] Prefetch blocks for cold range scans

## Bugs

- [ ] Persist the heap header on every allocation, not just on close (crash overwrites live rows)
- [ ] Flush dirty hot-cache chains and `msync` before WAL commit, or replay committed entries (crash loses committed writes)
- [ ] Persist or rebuild the Bloom filter on open (existing keys become unfindable after the first insert post-restart)
- [ ] Flush the hot cache before `hot_.clear()` in `DiskTrie::bulk_insert`
- [ ] Handle a missing light child when an insert diverges at a record-only node (null-pointer crash on prefix keys)
- [ ] Widen `split_bit` / `node_count` beyond `uint8_t` (keys over 32 bytes corrupt chains)
- [ ] Switch `UINT64` keys to big-endian (`RANGE` on numbers returns wrong rows; cluster routing too)
- [ ] Take `table_latch_` in `Table::insert`, `remove`, `bulk_insert`, `lookup`
- [ ] Dedupe keys within a `BULK` batch
- [ ] Rebalance (flip) after `bulk_insert` into a non-empty trie
- [ ] Keep `ChainCounts::total` correct on remove and on duplicate insert
- [ ] Prune empty nodes and chains on remove
- [ ] Persist free list entries beyond the ~1020 that fit in the header
- [ ] Lift the 4 GB mmap file-size limit
- [ ] Take `trie_latch_` before reading `bloom_` in `lookup`
- [ ] Guard `hot_` reads in `chain_read_shared` with `hot_mu_`
- [ ] Take `heap_mutex_` (or make mapping lock-free) in `HeapFile::read`
- [ ] Fix race on `repl_feed_threads` between the accept thread and shutdown
- [ ] Cap frame length in `recv_msg` / `recv_line`
- [ ] Add idle timeout so connections can't hold all 64 workers
- [ ] Update README/IMPROVEMENTS: range scan is no longer parallel; cluster hex boundaries after endianness fix
