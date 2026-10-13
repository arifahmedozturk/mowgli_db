# Where the time goes: inserts, lookups, range scans

Profile of master at `84ca154` (after #60), 1M random 64-bit keys, 64-byte values, single thread, AMD Ryzen 7 7435HS (WSL2). It was measured to choose the next optimisations; the resulting tasks are #73–#78 in [tasks.md](tasks.md).

## Method

Two passes, both on scratch builds that are not committed:

1. **Layer split.** A harness calls each layer separately on the same 1M keys: the whole `Table` operation, the bare `DiskTrie`, `HeapFile::read`, and row (de)serialisation. The difference between the table figure and the parts is glue (latches, argument conversions, result vectors).
2. **Phase timers.** `rdtsc` counters around the phases of `DiskTrie::insert_locked`, `lookup` and `match_in_place`, and around the `HeapFile` and `Table` calls, converted to ns with the measured TSC rate. The timers add some overhead, so the shares matter more than the exact nanoseconds; the instrumented insert runs at ~5.4 µs against ~4.8 µs without timers.

## Layer split

| Operation | Total | Index (trie) | Heap | Row encode/decode | Other |
|---|---|---|---|---|---|
| Insert | 4.8 µs | 3.2 µs | ~1.4 µs (with glue) | 0.2 µs | |
| Lookup | 1.8 µs | 1.1 µs | 0.27 µs | 0.06 µs | ~0.3 µs |
| Range scan, per key | 345 ns | 170 ns | 50 ns | 65 ns | ~60 ns |

The index is the largest cost in all three.

## Insert, by phase (~5.4 µs instrumented)

| Phase | Time | Share |
|---|---|---|
| Copy the target bucket out of the hot cache (`chain_read`) | 600 ns | 11% |
| Rebuild the bucket with the new entry (`bucket_insert_at` builds a new vector) | 440 ns | 8% |
| Put it back (`chain_write`: weight maps re-keyed, a flush to the file every 10th write) | 1,070 ns | 20% |
| **Bucket copy, edit, write back** | **2.1 µs** | **39%** |
| Heap placement (`has_room` 430 ns, `insert_near` 290 ns, amortised page splits) | 1,250 ns | 23% |
| Tail: Bloom filter, chain totals, `adjust_weights` (450 ns) | 690 ns | 13% |
| Descent through ~3.9 passed chains (`pass_through`) | 510 ns | 10% |
| Bucket binary search | 105 ns | 2% |
| Row serialisation | 100 ns | 2% |

## Lookup, by phase (~1.7 µs)

| Phase | Time |
|---|---|
| 4 hops (`match_in_place`), ~240 ns each | 980 ns |
| of which `hot_mu_` lock and `hot_.find` | ~25 ns per hop |
| of which matching on the cached `ChainData` | ~190 ns per hop |
| Bloom filter (7 probes, 7 cache misses for a key that exists) | 180 ns |
| Heap read (`heap_mutex_` and a copy into a new vector) | 230 ns |

At 1M keys the whole index fits in the hot cache (4,369 chains and buckets against a capacity of 13,500), so every hop matches on a decoded, separately allocated `ChainData` under `hot_mu_`. The zero-copy path on the mapped file is not used at this size.

## Range scan, per key (345 ns)

| Phase | Time |
|---|---|
| Index cursor (`advance_leftmost`) | 170 ns |
| Row deserialisation into `Row` vectors | 65 ns |
| Heap read | 50 ns |
| Result vectors and other glue | 60 ns |

Per key the cursor pops a frame, decodes the entry's suffix into a new vector, copies the prefix into another, pushes a frame back and builds the key as a new `std::string`: about three allocations per key.

## Weak points, ranked

| # | Weak point | Cost | Task |
|---|---|---|---|
| 1 | Buckets are copied to be edited: copy out of the cache, rebuild, copy back. Fix: edit them in place in their mapped slot and cache decoded chains only | 2.1 µs per insert (39%) | #73 |
| 2 | Heap pages have no room bookkeeping: `has_room`, `make_room` and `write_row` scan the whole slot directory, 2–3 times per insert | part of 1.25 µs per insert (23%) | #74 |
| 3 | The range cursor allocates about three times per key | 170 ns per key (half a scan) | #75 |
| 4 | The Bloom filter's 7 probes are 7 cache misses | 180 ns per lookup and per insert | #76 |
| 5 | Global locks on the read path: `hot_mu_` per hop, `heap_mutex_` per heap read, `grow_mutex_` per pin | small single-threaded; serialises readers | #77, #16 |
| 6 | Chain totals and weights live in a separate hash map, looked up per frame and per visited chain | 690 ns per insert (13%) | #78 |

#73 also helps durability: bucket edits reach the file mapping immediately instead of sitting in the hot cache until a flush (#28).
