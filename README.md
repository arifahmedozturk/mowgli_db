# heavy-trie

heavy-trie is a from-scratch, disk-backed key-value store and small database engine. Instead of a B-tree, the standard disk index, it uses a **radix trie with heavy-path decomposition** as its primary index, and asks whether that classic competitive-programming technique can give a disk-resident trie B-tree-class lookup guarantees while keeping a trie's native prefix and ordered-range queries.

**The core idea:** at every branching point, the child with the most keys beneath it is the *heavy child*. Following heavy children traces a *heavy path*, and each heavy path is stored as one compact on-disk *chain*. Crossing to a light child at least halves the remaining keys, so any lookup crosses at most **O(log n) chains**, the same bound as a B-tree's page reads. The trie branches on 4-bit digits (base 16), and once a subtree is small enough to fit in a block it is stored as a single sorted **bucket**, so a lookup ends with one binary search and a range scan reads a few adjacent buckets.

Around the index is a complete small engine: an mmap-backed storage layer, a write-back chain cache, a WAL, a SQL-like query language, a TCP server with key-range sharding and primary/replica replication, and a benchmark suite that compares it against LMDB and SQLite.

## Contents

- [Results](#results)
  - [Against embedded B-trees](#against-embedded-b-trees)
  - [YCSB](#ycsb)
  - [What the optimisation work changed](#what-the-optimisation-work-changed)
- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Build and test](#build-and-test)
  - [REPL](#repl)
  - [TCP server and client](#tcp-server-and-client)
  - [Two-node cluster](#two-node-cluster)
  - [Primary/replica replication](#primaryreplica-replication)
  - [Benchmarks](#benchmarks)
- [Documentation](#documentation)
  - [Index design](#index-design)
  - [Storage on disk](#storage-on-disk)
  - [Updates and rebalancing](#updates-and-rebalancing)
  - [Caching and startup](#caching-and-startup)
  - [Compaction](#compaction)
  - [Concurrency and durability](#concurrency-and-durability)
  - [Query language](#query-language)
  - [Server and wire protocol](#server-and-wire-protocol)
  - [Benchmark suite](#benchmark-suite)
  - [Project structure](#project-structure)
  - [Future work](#future-work)

## Results

### Against embedded B-trees

Random 64-bit keys with 64-byte values, single thread, all three engines configured alike: no fsync or journal, reads through mmap, one transaction per single insert, and range scans copy each row's columns into reused buffers. Each figure is the **median of 15 runs at 1M keys** and of **5 runs at 10M keys** on an AMD Ryzen 7 7435HS (WSL2). Medians are used because a few runs were disturbed by background load; most runs fall within 10% of the median, and tail latencies and table-open times vary the most. Reproduce with `bench/run.sh --reps 15 --n 1000000 --no-ycsb` and `bench/run.sh --reps 5 --n 10000000 --no-ycsb`.

**1M keys**

| | heavy-trie | LMDB (B+tree) | SQLite (B-tree) |
|---|---|---|---|
| Point lookups / s | **846K** | 1.13M | 101K |
| Lookup latency p50 / p99 | 1.1 / 1.9 µs | 0.8 / 1.5 µs | 9.6 / 19 µs |
| Single inserts / s | **359K** | 603K | 47K |
| Insert latency p50 / p99 | 1.7 / 32 µs | 1.3 / 8.0 µs | 18.9 / 49 µs |
| Bulk load / s | 905K | 2.91M | 934K |
| Range scans / s (1,000 keys each) | **10.8K** | 23.1K | 5.4K |
| Bytes per key (index + rows) | 135 (36 + 100) | 122 | 90 |
| Open an existing table | 4.4 ms | 0.2 ms | 0.2 ms |

**10M keys**

| | heavy-trie | LMDB (B+tree) | SQLite (B-tree) |
|---|---|---|---|
| Point lookups / s | **388K** | 615K | 87K |
| Lookup latency p50 / p99 | 2.3 / 5.3 µs | 1.5 / 3.2 µs | 11.0 / 25 µs |
| Single inserts / s | **181K** | 412K | 42K |
| Insert latency p50 / p99 | 3.1 / 38 µs | 1.9 / 10 µs | 20.9 / 60 µs |
| Bulk load / s | 610K | 1.76M | 739K |
| Range scans / s (10,000 keys each) | **1,125** | 2,102 | 549 |
| Bytes per key (index + rows) | 158 (57 + 100) | 123 | 90 |
| Open an existing table | 74 ms | 0.2 ms | 0.2 ms |

heavy-trie is 4.5–8.4× faster than SQLite on point lookups, 4.3–7.6× faster on single inserts and twice as fast on range scans; SQLite loads in bulk faster (by 3% at 1M and 21% at 10M). LMDB is 1.3× faster on lookups at 1M and 1.6× at 10M, 1.7–2.3× faster on inserts, about 2× faster on range scans and about 3× faster on bulk loads. heavy-trie also uses the most space, mainly because its row heap keeps rows in key order and splits pages like B-tree leaves, so pages are not packed full (100 bytes per key of rows, against 79 when rows were simply appended). Opening a table loads the Bloom filter's sidecar file, which grows with the table (task #72).

### YCSB

The standard YCSB workloads on 1M records of 10 fields × 100 bytes, 100K operations each, keys chosen with a Zipfian distribution (θ = 0.99); scans in workload E cover 1–100 records. Medians of 5 runs for heavy-trie and 3 for PostgreSQL 14 (measured earlier; nothing on its side has changed). Reproduce with `./build/ycsb 1000000 --ops 100000` and `./build/ycsb_pg 1000000 --ops 100000 --async`.

| Workload | Mix | heavy-trie | PostgreSQL |
|---|---|---|---|
| A | 50% reads, 50% updates | **257K ops/s** | 7.7K |
| B | 95% reads, 5% updates | **724K** | 8.4K |
| C | 100% reads | **714K** | 8.6K |
| D | 95% reads of recent keys, 5% inserts | **631K** | 9.0K |
| E | 95% short scans, 5% inserts | **28K** | 3.8K |
| F | 50% reads, 50% read-modify-writes | **840K** | 6.5K |
| Load | 1M records | 259K records/s | 47K |
| Read latency p50 / p99 (workload C) | | 1.5 / 3.0 µs | 110 / 202 µs |

The two are not alike: heavy-trie runs in the benchmark's process with no write-ahead log, while PostgreSQL is a client–server database reached through libpq over a local socket, here with `synchronous_commit = off`. Most of PostgreSQL's ~110 µs per operation is that round trip, so these figures show what an embedded engine avoids rather than comparing storage engines; the B-tree tables above are the like-for-like comparison. Raw logs are in `bench/results/20261018-ycsb_65ca2c5/` (heavy-trie) and `bench/results/20261014-ycsb_eb31f9f/` (PostgreSQL).

### What the optimisation work changed

The same 1M-key benchmark, 15 runs each, before the latest rounds of work (binary trie, one chain per key, rows appended in insertion order) and now:

| | Before | Now | Change |
|---|---|---|---|
| Point lookups | 181K/s | 846K/s | 4.7× |
| Single inserts | 56K/s | 359K/s | 6.4× |
| Bulk load | 186K/s | 905K/s | 4.9× |
| Range scans | 371/s | 10.8K/s | 29×, partly from the benchmark copying rows out instead of building them (#64) |
| Index bytes per key | 454 | 36 | 13× smaller |
| Index bytes per key after compaction | 607 | 34 | 18× smaller |
| Open an existing table | 1.42 s | 4.4 ms | 325× faster |
| Compaction | 4.5 s | 118 ms | 38× faster |

The main steps were matching chains directly on the mapped bytes instead of decoding them, persisting startup state instead of rebuilding it, moving from base 2 to base 16, subtree buckets, keeping heap rows in key order so range scans read neighbouring rows from the same page, editing buckets in place in the mapped file, a range cursor that does not allocate per key, range scans that read rows where they lie in the heap instead of building them, and bulk loads that sort an index of the rows instead of the rows themselves. Keeping rows in order costs space: 135 bytes per key, against 115 when rows were appended. The raw results are in `bench/results/` (`20261018-192929_65ca2c5_1M-x15.json`, `20261018-203835_65ca2c5_10M-x5.json`, `*_1M-x15-before.json`), and every step has its own before/after run there; the earlier history is in [IMPROVEMENTS.md](IMPROVEMENTS.md).

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

The server accepts statements over TCP and runs `COMPACT` on all tables after 30 seconds of idle, then waits 5 minutes before compacting again. A connection that sends nothing (or stops reading replies) for `--idle-timeout` seconds, 300 by default, is closed so it can't hold a worker thread; `0` turns this off. Each write is logged to the WAL and acknowledged once its log records are on disk, with concurrent writers sharing one `fdatasync` (group commit); `--wal-sync off` skips the sync, which still survives the server process crashing but not the machine losing power. The client is a readline REPL.

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
./build/bench_wal 10000                     # writes with the WAL synced, unsynced and off
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

**Packing.** Chains and buckets up to 4 KB share packed blocks (up to 255 slots per block), addressed as `(slot << 56) | block`; larger ones get a dedicated block. Each slot is rounded up to a size class, a power of two from 64 bytes to 4 KB, which leaves room to grow in place. A bucket that outgrows its slot moves to a larger class or a dedicated block, and its parent's pointer is updated. A chain that outgrows its slot while being flushed from the cache, where its parent isn't known, leaves a forwarding stub that points straight at the data instead. A freed slot keeps its place in the block and is reused by the next record that fits.

**Heap file.** Rows are stored in a separate slotted-page heap file; the trie maps each key to a heap record pointer. Each page holds a contiguous key range, so a range scan reads neighbouring rows from the same page. A new row goes on the page of its key neighbour; when that page is full, its highest rows shift into the next page if it has room, otherwise the two pages split into three (B*-tree style), and the moved keys are repointed in the trie. A delete that leaves a page sparse merges the next page into it when that page's rows fit, and the space of deleted rows is reused. Both files are mmap-backed in a 256 GiB virtual reservation (no memory cost) and grow in 8 MB steps.

**Reads.** Lookups match each chain or bucket directly on the mapped bytes, with no decoding or allocation per step; inserts and removes do the same for the chains they only pass through and decode only the chains they change. A Bloom filter rejects most missing keys before any trie access. It keeps all of a key's bits in one 64-byte block, so a check costs one memory access per stage, and it grows in stages of about 10 bits per key (around 1% false positives) as the table grows. It is saved on clean shutdown and rebuilt after a crash.

### Updates and rebalancing

- **Insert:** walks the trie like a lookup. A new branch gets a one-key bucket as its light child; a key that lands in a bucket is inserted into the bucket's bytes where they sit in the mapped file.
- **Flip:** when a light child outgrows the heavy side, the two swap roles. The light chain's path is spliced into the parent chain and the old heavy tail becomes a new light chain. Weights are stored as `floor(log2(count))`, so nodes near the root are rewritten very rarely.
- **Burst:** a bucket that outgrows the block size is rebuilt in place as a chain with bucket children.
- **Merge:** a chain subtree that shrinks to 64 keys or fewer collapses back into one bucket.
- **Remove:** a light subtree left with no keys is unlinked and freed, and a node left with neither a key nor a light child is dropped from its chain.
- **Bulk load:** into an empty table, the whole structure is built bottom-up from the sorted keys in one pass.

### Caching and startup

`DiskTrie` keeps a write-back cache of up to 13,500 decoded chains, evicting the lightest (fewest keys) clean entries first. Chain writes go to the cache and are flushed after 10 updates or on eviction. Buckets are not cached: they are read and edited where they live in the mapped file. Opening a table does no full scan: the Bloom filter is loaded from its sidecar file, and the per-chain counts used for rebalancing are rebuilt only on the first write.

### Compaction

`COMPACT(table)`, also run by the server when idle, rewrites every chain and bucket in depth-first order so each sits near its parent, frees the old slots, then slides the new layout down to the start of the file and truncates the rest, so compaction gives the old layout's space back. `compact_lex` instead lays them out in key order, for range scans. The slide overwrites blocks in place, so a crash during compaction can corrupt the index.

### Concurrency and durability

Each table has a reader/writer latch: lookups and scans share it, and writes take it exclusively. The trie and heap have their own internal latches, so both stay consistent under concurrent use. The engine logs each mutating command to a CRC32-checked write-ahead log before applying it and replays unfinished commands on startup.

### Query language

The REPL and the server speak a small SQL subset. Keywords are case-insensitive, and a statement may end with `;`.

| Type | Description |
|---|---|
| `NUMBER` (`INT`, `BIGINT`) | Unsigned 64-bit integer, stored big-endian, so numeric order is key order |
| `VARCHAR(n)` (`STRING`, `TEXT`) | Up to `n` bytes (64 by default, at most 1024) |

Exactly one column per table must be the `PRIMARY KEY`. Values are numbers or `'strings'`, with `''` for a quote inside a string; `x'00ff'` is a string of any bytes, two hex digits each, and `SELECT` prints a string that is not text (valid UTF-8 without control characters) in that form.

| Statement | Notes |
|---|---|
| `CREATE TABLE t (col type [PRIMARY KEY], ...)` | Creates `.trie`, `.heap` and `.schema` files |
| `DROP TABLE t` | Removes the table and its files |
| `SHOW TABLES`, `DESCRIBE t` | |
| `CREATE INDEX i ON t (col, ...)` | A secondary index, built over the existing rows and kept current by every write |
| `DROP INDEX i ON t` | |
| `INSERT INTO t VALUES (v, ...), (v, ...)` | A row whose key is stored is skipped; several rows go in as one bulk load |
| `SELECT * \| col, ... \| COUNT(*) FROM t [WHERE ...] [LIMIT n]` | Rows in key order |
| `UPDATE t SET col = v, ... [WHERE ...]` | Only the named columns change; the key cannot |
| `DELETE FROM t [WHERE ...]` | |
| `COMPACT t` | Depth-first repack of the index |
| `SHOW CHAINS t` | Active vs. allocated chains |
| `STATS` | In-flight requests and per-table cache hit rate |

A `WHERE` clause is conditions joined by `AND`: `col = v`, `<`, `<=`, `>`, `>=`, `col BETWEEN v AND w` (inclusive), `col IN (v, ...)` and, for string columns, `STARTS_WITH(col, 'prefix')`. Conditions on the primary key decide what is read: `=` and `IN` read only those keys, and bounds and `STARTS_WITH` read only that key range, the trie's natural prefix query. With no condition on the key, a secondary index is used if conditions constrain its leading columns: `=` or `IN` on each of them, then optionally a range on the next one; the index whose usable prefix is longest wins, and its rows come back in index order. Every condition is then checked on each row read, so a clause that neither the key nor an index serves reads the whole table. Writes report how many rows they changed, as `INSERT n`, `UPDATE n` and `DELETE n`.

```
CREATE TABLE users (id VARCHAR(16) PRIMARY KEY, age NUMBER, name TEXT)

INSERT INTO users VALUES ('alice', 30, 'Alice Smith'), ('bob', 25, 'Bob Jones'), ('carol', 28, 'Carol White')
→ INSERT 3

SELECT * FROM users WHERE id = 'alice'
→ alice | 30 | Alice Smith
(1 rows)

SELECT id, age FROM users WHERE id BETWEEN 'alice' AND 'bob'
→ alice | 30
→ bob | 25
(2 rows)

UPDATE users SET age = 31 WHERE id = 'alice'
→ UPDATE 1

CREATE INDEX by_age ON users (age)
SELECT name FROM users WHERE age < 30
→ Bob Jones
→ Carol White
(2 rows)

SELECT id FROM users WHERE STARTS_WITH(id, 'c')
→ carol
(1 rows)

DELETE FROM users WHERE id IN ('bob', 'nobody')
→ DELETE 1

SELECT COUNT(*) FROM users
→ 2
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
| `bench_wal` | Writes through the engine with the WAL synced, unsynced and off |
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
├── mql/                      Lexer, parser and the engine that runs statements
├── server/                   TCP server, client, cluster routing, replication, wire framing
├── bench/                    Benchmarks, run/compare scripts, results/
├── tests/                    Unit, model-based and concurrency tests
├── IMPROVEMENTS.md           Earlier optimisation history
└── tasks.md                  Open work
```

### Future work

Open items are tracked in [tasks.md](tasks.md). The larger directions:

- **Range scans** are still about 2× behind LMDB (task #110).
- **Power-loss durability:** a committed write survives the process crashing, but the table files are not synced at commit, so a power loss can lose committed writes or tear the files; this needs checkpoints and page images in the log (#113).
- **Finer-grained write concurrency** than one latch per table (#25), and a multi-threaded benchmark (#4).
- **Server hardening:** authentication and TLS.
- **Query features:** prepared statements, server-side `WHERE` filters, composite keys, multi-table transactions.
- **Cluster rebalancing:** moving key ranges between nodes as load shifts.
