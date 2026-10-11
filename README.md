# heavy-trie

heavy-trie is a from-scratch, disk-backed key-value store and small database engine. Instead of a B-tree, the standard disk index, it uses a **radix trie with heavy-path decomposition** as its primary index, and asks whether that classic competitive-programming technique can give a disk-resident trie B-tree-class lookup guarantees while keeping a trie's native prefix and ordered-range queries.

**The core idea:** at every branching point, the child with the most keys beneath it is the *heavy child*. Following heavy children traces a *heavy path*, and each heavy path is stored as one compact on-disk *chain*. Crossing to a light child at least halves the remaining keys, so any lookup crosses at most **O(log n) chains**, the same bound as a B-tree's page reads. The trie branches on 4-bit digits (base 16), and once a subtree is small enough to fit in a block it is stored as a single sorted **bucket**, so a lookup ends with one binary search and a range scan reads a few adjacent buckets.

Around the index is a complete small engine: an mmap-backed storage layer, a write-back chain cache, a WAL, a text query language (MQL), a TCP server with key-range sharding and primary/replica replication, and a benchmark suite that compares it against LMDB and SQLite.

## Results

### Against embedded B-trees

Random 64-bit keys with 64-byte values, single thread, all three engines configured alike: no fsync or journal, reads through mmap, one transaction per single insert. Each figure is the mean of **15 runs at 1M keys** and **5 runs at 10M keys** on an AMD Ryzen 7 7435HS (WSL2). Run-to-run variation is under 5% for nearly every figure (p99 latencies 9–13%, LMDB's 10M range scans 25%). Reproduce with `bench/run.sh --reps 15 --n 1000000`.

| 1M keys · 10M keys | heavy-trie | LMDB (B+tree) | SQLite (B-tree) |
|---|---|---|---|
| Point lookups / s | **700K** · **338K** | 1.04M · 616K | 95K · 87K |
| Lookup latency p50 / p99 (1M) | 1.3 / 2.6 µs | 0.9 / 1.7 µs | 9.9 / 24 µs |
| Single inserts / s | **326K** · **145K** | 570K · 373K | 45K · 41K |
| Bulk load / s | 594K · 381K | 2.72M · 1.71M | 918K · 721K |
| Range scans / s (N/1000 keys each) | 2,152 · 161 | 22.6K · 1.9K | 5.2K · 553 |
| Bytes per key (index + rows) | 115 · 136 | 122 · 123 | 90 · 90 |
| Open an existing table | 3.6 ms · 49 ms | 0.2 ms · 0.3 ms | 0.2 ms · 0.2 ms |

heavy-trie is 3.5–7× faster than SQLite on point lookups and single inserts, and within 1.5–1.8× of LMDB on lookups (1.7–2.6× on inserts). Its footprint is comparable to LMDB's (115–136 vs 122 bytes per key; SQLite is smaller at 90). Range scans are the remaining gap, 10–12× behind LMDB.

### What the optimisation work changed

The same 1M-key benchmark, 15 runs each, before the latest round of work (binary trie, one chain per key) and now (base-16 trie with subtree buckets):

| | Before | Now | Change |
|---|---|---|---|
| Point lookups | 180K/s | 700K/s | 3.9× |
| Single inserts | 56K/s | 326K/s | 5.9× |
| Range scans | 370/s | 2,152/s | 5.8× |
| Index bytes per key | 454 | 36 | 12.7× smaller |
| Open an existing table | 1.44 s | 3.6 ms | 400× faster |
| Compaction | 4.5 s | 79 ms | 58× faster |

The main steps were matching chains directly on the mapped bytes instead of decoding them, persisting startup state instead of rebuilding it, moving from base 2 to base 16 (chosen with a simulation in [trie-base-investigation.md](trie-base-investigation.md)), and subtree buckets. The raw results for both tables are in `bench/results/` (`*_1M-x15.json`, `*_10M-x5.json`, `*_1M-x15-before.json`), and every step has its own before/after run there; the earlier history is in [IMPROVEMENTS.md](IMPROVEMENTS.md).

---

## Setup

### Prerequisites

| Dependency | Purpose | Install |
|---|---|---|
| g++ ≥ 11 / clang++ ≥ 14 | C++20 required | `sudo apt install build-essential` |
| CMake ≥ 3.20 | Build system | `sudo apt install cmake` |
| libreadline-dev | REPL and client history | `sudo apt install libreadline-dev` |
| liblmdb-dev, libsqlite3-dev *(optional)* | B-tree baselines (`bench_btree`) | `sudo apt install liblmdb-dev libsqlite3-dev` |
| libpq-dev, PostgreSQL *(optional)* | PostgreSQL YCSB comparison | `sudo apt install libpq-dev postgresql` |

### Build and test

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
cd build && ctest --output-on-failure
```

`bench_btree` and `ycsb_pg` are built only when their optional libraries are found.

### REPL

```bash
./build/mql              # data stored in ./data/
./build/mql ./mydb       # custom data directory
```

Data persists across restarts. Each table is stored as `.trie`, `.heap` and `.schema` files in the data directory (plus a `.trie.bloom` file written on clean shutdown).

### TCP server and client

```bash
./build/server                        # port 5432, data in ./data/
./build/server --port 9000 --data /var/db/heavy

./build/client                        # connects to 127.0.0.1:5432
./build/client --host 10.0.0.1 --port 9000
```

The server accepts MQL commands over TCP and runs `COMPACT` on all tables after 30 seconds of idle, then waits 5 minutes before compacting again. The client is a readline REPL.

### Two-node cluster

Give each node a key range in a shared config file:

```
# cluster.conf — one line per node: host port lo_hex hi_hex
# "-" means open-ended (absolute min / absolute max)
127.0.0.1 5432 - 8000000000000000
127.0.0.1 5433 8000000000000000 -
```

```bash
./build/server --port 5432 --data ./data0 --cluster cluster.conf
./build/server --port 5433 --data ./data1 --cluster cluster.conf
```

Single-key commands (`NEW`, `QUERY`, `DELETE`, `UPDATE`) are routed to the node that owns the key; `RANGE` runs locally and on every overlapping peer, and the results are merged in key order. Clients can connect to any node. Boundaries are hex bytes in the engine's key encoding: `number` keys are big-endian, so the midpoint of the uint64 space is `8000000000000000`; `string` keys use their raw bytes, e.g. `6d` (`'m'`).

### Primary/replica replication

```bash
./build/server --port 5432 --data ./primary --repl-port 5532          # primary
./build/server --port 5433 --data ./replica --primary 127.0.0.1:5532  # replica
```

The replica streams all mutations from the primary, forwards writes to it, and serves reads locally. After a crash it resumes from its last applied log position. The replication log uses the same CRC32-verified framing as the WAL.

### Benchmarks

```bash
./build/bench 1000000                       # bulk load, inserts, lookups, ranges, compaction, cold start
./build/bench 1000000 --keys prefix_str     # key sets: random_u64 (default), seq_u64, prefix_str, varlen_str
./build/bench_btree 1000000                 # the same workload on LMDB and SQLite
./build/bench_wal 10000                     # MQL writes with the WAL on vs off
./build/ycsb 1000000 --ops 500000           # YCSB workloads A–F

# Repeatable runs: Release build, N repetitions, mean/stddev + machine info -> bench/results/*.json
bench/run.sh --reps 3 --n 1000000 --label my-change
bench/compare.py bench/results/<before>.json bench/results/<after>.json

# PostgreSQL YCSB comparison (needs a running Postgres):
sudo service postgresql start
sudo -u postgres createuser --superuser $USER
createdb ycsb_bench
./build/ycsb_pg            # sync writes
./build/ycsb_pg --async    # SET synchronous_commit=off
```

---

## Documentation

### Index design

**Radix trie, base 16.** Keys are byte strings walked one 4-bit digit (nibble) at a time. Runs of digits with no branching are path-compressed, so depth depends on the number of branch points, not on key length.

**Heavy-path decomposition.** Every node's weight is the number of keys below it. At each branch the heaviest child is the *heavy child* and the others (up to 15) are *light children*. A maximal run of heavy edges is a *chain*: one on-disk record holding the path digits, the branch points along it, and the addresses of the light children hanging off them. A light child holds at most half its parent's keys, so a lookup crosses at most O(log n) chains. A lookup crosses 4 on average at 1M random keys and 5 at 10M.

**Subtree buckets.** A subtree whose keys fit in a block (8 KB by default) is stored as a single sorted bucket: each key's remaining digits plus its record pointer, with an offset table for binary search. Buckets are always leaves. A chain stops where its heavy side would fit in a bucket and ends in an *end node* that fans out into bucket children. Most keys therefore live in buckets, which makes range scans sequential within a bucket and cuts the index to 36 bytes per key at 1M random keys (57 at 10M).

**Why not a B-tree?** B-trees are the standard for good reasons, and this project isn't an argument that tries are better. Tries offer different things: ordering and prefix queries fall out of the structure, keys with long shared prefixes are stored once along a path, and the tree rebalances through local *flips* driven by weights rather than page splits and merges. The benchmarks above measure what that costs and buys.

### Storage on disk

**Trie file.** A flat file of 8 KB blocks managed by `DiskManager`. Block 0 is the header: root address, key count, allocation high-water mark and the start of the free list (overflow free-list pages hold IDs that don't fit in the header). Other blocks are dedicated blocks (one large chain or bucket) or packed blocks (many small ones).

**Chain format.** A 19-byte header (path length, node count, start-digit parity, tail record), the path packed two digits per byte, then per branch node: its position, the heavy side's weight, a 16-bit bitmap of which digits have light children, the record of a key ending there (only at byte boundaries), and 10 bytes per light child (address + weight). A child is found with `popcount(bitmap & ((1 << digit) - 1))`, with no scanning.

**Bucket format.** A 10-byte header, a table of 2-byte entry offsets, then the entries in sorted order: digit count, record pointer, digits packed two per byte. Inserts and deletes edit the encoded bytes in place.

**Packing.** Chains and buckets up to 256 bytes share packed blocks, 27 slots per block, addressed as `(slot << 56) | block`. A record that outgrows its slot moves to a dedicated block and leaves a forwarding stub, so parent pointers stay valid.

**Heap file.** Rows are stored in a separate slotted-page heap file; the trie maps each key to a heap record pointer. Both files are mmap-backed in a 256 GiB virtual reservation (no memory cost) and grow in 8 MB steps.

**Reads.** Lookups match each chain or bucket directly on the mapped bytes, with no decoding or allocation per step. A 512 KB Bloom filter rejects most missing keys before any trie access; it is saved on clean shutdown and rebuilt after a crash.

### Updates and rebalancing

- **Insert:** walks the trie like a lookup. A new branch gets a one-key bucket as its light child; a key that lands in a bucket is inserted into it in place.
- **Flip:** when a light child outgrows the heavy side, the two swap roles. The light chain's path is spliced into the parent chain and the old heavy tail becomes a new light chain. Weights are stored as `floor(log2(count))`, so nodes near the root are rewritten very rarely.
- **Burst:** a bucket that outgrows the block size is rebuilt in place as a chain with bucket children.
- **Merge:** a chain subtree that shrinks to 64 keys or fewer collapses back into one bucket.
- **Bulk load:** into an empty table, the whole structure is built bottom-up from the sorted keys in one pass.

### Caching and startup

`DiskTrie` keeps a write-back cache of up to 13,500 chains and buckets, evicting the lightest (fewest keys) clean entries first. Writes go to the cache and are flushed after 10 updates or on eviction. Opening a table does no full scan: the Bloom filter is loaded from its sidecar file, and the per-chain counts used for rebalancing are rebuilt only on the first write.

### Compaction

`COMPACT(table)`, also run by the server when idle, rewrites every chain and bucket in depth-first order so each sits near its parent, then frees the old slots. `compact_lex` instead lays them out in key order, for range scans.

### Concurrency and durability

Each table has a reader/writer latch: lookups and scans share it, and writes take it exclusively. The trie and heap have their own internal latches, so both stay consistent under concurrent use. The engine logs each mutating command to a CRC32-checked write-ahead log before applying it and replays unfinished commands on startup.

### MQL reference

MQL (Mini Query Language) is the text protocol of both the REPL and the server.

| Type | Description |
|---|---|
| `string` | Variable-length bytes, stored as VARCHAR |
| `number` | Unsigned 64-bit integer (stored big-endian, so numeric order = key order) |

Exactly one column per table must be the `PRIMARY KEY`.

| Command | Syntax | Notes |
|---|---|---|
| Create table | `TABLE name(col type [PRIMARY KEY], ...)` | Creates `.trie`, `.heap`, `.schema` files |
| Insert row | `NEW(table, val, ...)` | Error if the key exists |
| Bulk insert | `BULK table (val, ...) (val, ...) ...` | Builds the index bottom-up when the table is empty |
| Update row | `UPDATE(table, val, ...)` | Full row replacement by key |
| Delete row | `DELETE(table, pk)` | |
| Point query | `QUERY(table, pk)` | Also reports the chains crossed |
| Range scan | `RANGE(table, lo, hi)` | Inclusive on both ends |
| IN lookup | `IN(table, k1, k2, ...)` | Matching rows in input order |
| Count | `COUNT(table)` | |
| Chain stats | `CHAINS(table)` | Active vs. allocated chains |
| Compact | `COMPACT(table)` | Depth-first repack |
| Server stats | `STATS` | In-flight requests + per-table cache hit rate |

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
```

### Server and wire protocol

Requests and responses are length-prefixed text frames:

```
Request:   "<len>\n<command>\n"
Response:  "<len>\n<result>\n"
```

Each connection reads through a buffered `FrameReader`, so one `read()` can carry many frames, and each frame is sent with a single `writev`. Frame size is capped. The framing code is in [server/wire.h](server/wire.h). The server counts in-flight requests so idle compaction only starts when nothing is running. On `SIGINT`/`SIGTERM` it drains in-flight requests (up to 5 s) and exits cleanly.

### Benchmark suite

| Program | What it measures |
|---|---|
| `bench` | Bulk load, single inserts, lookups (with p50/p99/p999), range scans, compaction, startup, cold-cache start, chains and blocks per lookup, bytes per key; four key distributions |
| `bench_btree` | The same workload on LMDB and SQLite (`WITHOUT ROWID`) |
| `bench_wal` | MQL writes through the engine with the WAL on vs off |
| `ycsb` / `ycsb_pg` | YCSB workloads A–F (Zipfian θ = 0.99) on heavy-trie and on PostgreSQL |
| `bench/run.sh` | All of the above, N repetitions, mean/stddev and machine info to `bench/results/*.json`; `compare.py` diffs two runs and flags changes beyond 2× stddev |

### Project structure

```
heavy-trie/
├── main.cpp                  REPL entry point
├── index/
│   ├── chain.h/cpp           Chain and bucket encoding, zero-copy matching
│   └── disk_trie.h/cpp       Insert/lookup/remove/range, flips, bursts/merges, cache, compaction
├── storage/
│   ├── buffer_pool.h/cpp     mmap-backed block access with stripe latches
│   ├── disk_manager.h/cpp    Block allocation, free list, packed slots
│   ├── heap.h/cpp            Slotted-page row storage
│   └── wal.h/cpp             Write-ahead log
├── catalog/table.h/cpp       Schema + trie + heap
├── mql/                      Lexer and executor
├── server/                   TCP server, client, cluster routing, replication, wire framing
├── bench/                    Benchmarks, run/compare scripts, results/
├── tests/                    Unit, model-based and concurrency tests
├── trie-base-investigation.md  Base 2 vs 4/16/256 study behind the base-16 switch
├── IMPROVEMENTS.md           Earlier optimisation history
└── tasks.md                  Open work
```

### Future work

Open items are tracked in [tasks.md](tasks.md). The larger directions:

- **Range scans** are still 10–12× behind LMDB (task #64).
- **WAL group commit:** each logged write currently pays two `fdatasync` calls (#21).
- **Finer-grained write concurrency** than one latch per table (#25), and a multi-threaded benchmark (#4).
- **Variable slot sizes** for mid-sized buckets, which today take a whole block (#18).
- **Server hardening:** authentication/TLS and idle-connection timeouts (#46).
- **Query features:** prepared statements, server-side `WHERE` filters, composite keys, multi-table transactions.
- **Cluster rebalancing:** moving key ranges between nodes as load shifts.
