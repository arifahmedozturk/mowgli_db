# heavy-trie

heavy-trie is a from-scratch disk-backed key-value store. Rather than building on a B-tree — the standard choice for disk indexes — it explores an alternative: a **binary trie with heavy-path decomposition** as the primary index structure.

The motivating question is whether applying a classic algorithm (heavy-light decomposition, common in competitive programming for tree problems) to a disk-resident trie can match B-tree lookup guarantees while gaining native support for prefix and lexicographic range queries. The answer turns out to be yes, with some interesting engineering trade-offs along the way.

**The core idea:** at every branching node in the trie, the child with more keys beneath it is the *heavy child*. Following heavy children from the root traces a heavy path. Any root-to-leaf traversal crosses at most O(log n) such paths — and each path is packed into a single 8 KB block. A point lookup therefore reads at most **O(log n) blocks**, the same as a B-tree. Range scans are lexicographic by construction, falling out of the trie's structure without any extra sorted-leaf machinery.

On top of the index the project builds a complete small database engine:
- A **mmap-backed storage layer** with 4 GB virtual reservation per file, stripe-latched buffer pool, and pre-growth in 8 MB chunks.
- A **write-back hot-chain cache** that amortises repeated reads/writes to the same block during insert-heavy workloads.
- A **multi-chain block packing** scheme that stores up to 27 small chains per 8 KB block, reducing physical block count by 17× for a typical 100K-record table.
- A **parallel range scan** that dispatches light subtrees as async tasks while the heavy path continues inline.
- A **bulk-load path** that suppresses per-insert bookkeeping and rebuilds weights in a single DFS pass — 4× faster than sequential inserts.
- A **compaction pass** that rewrites chain blocks in DFS pre-order, halving lookup I/O after a write-heavy period.
- A **TCP server** with a framed text protocol, per-connection threads, and a background compaction thread that fires after 30 seconds of idle.
- A **P2P cluster layer** that shards writes across N nodes by key range, routing single-key ops to the owning node and scatter-gathering range scans across all overlapping nodes.
- A **streaming replication** channel (primary/replica) backed by a CRC32-verified append-only log; replicas forward writes to the primary and serve reads locally.
- A **YCSB benchmark harness** (workloads A–F) with a PostgreSQL comparison driver for apples-to-apples numbers.

The full implementation history — each optimisation step with before/after numbers — is in [IMPROVEMENTS.md](IMPROVEMENTS.md).

### Key results

| Metric | Value |
|---|---|
| Point lookup I/O | O(log n) block reads |
| Physical blocks vs. naive trie | **17× fewer** (multi-chain packing) |
| Range scan throughput improvement | **+234×** (branch pruning) |
| Bulk load vs. sequential inserts | **+4×** (single-pass weight rebuild) |
| Lookup improvement after compaction | **+2×** (DFS pre-order repack) |

---

## Why not a B-tree?

B-trees are the standard disk index for a reason: they keep data sorted in leaf pages and provide O(log n) lookups and sequential range scan I/O. This project is not an argument that tries are *better* — it is an exploration of the design space.

Tries offer things B-trees do not:
- **Prefix queries** are structural. Descending to a prefix node returns all keys with that prefix without scanning a potentially large sorted range.
- **Lexicographic range scans** are in-order trie traversal. There is no need for a sorted leaf layer; the ordering is implicit in the bit encoding.
- **No rotation or split/merge logic.** The tree re-organises through local flip operations triggered by weight changes, not by page occupancy thresholds.

The cost traditionally is space: a naive trie stores one node per key bit, wasting enormous amounts of disk on nearly-empty blocks. The multi-chain packing scheme here addresses that directly, reducing block count 17× so the per-block utilisation is competitive with B-tree leaf pages.

The result is a structure with B-tree-level I/O guarantees, better structural support for certain query types, and a fundamentally different internal design — worth studying as a reference implementation of these ideas in a production-adjacent context.

---

## Setup

### Prerequisites

| Dependency | Purpose | Install |
|---|---|---|
| g++ ≥ 11 / clang++ ≥ 14 | C++20 required | `sudo apt install build-essential` |
| CMake ≥ 3.20 | Build system | `sudo apt install cmake` |
| libreadline-dev | REPL and client history | `sudo apt install libreadline-dev` |
| libpq-dev *(optional)* | PostgreSQL YCSB harness | `sudo apt install libpq-dev` |
| PostgreSQL *(optional)* | PostgreSQL YCSB target | `sudo apt install postgresql` |

### Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

`ycsb_pg` is built automatically if libpq-dev is found; otherwise it is silently skipped.

### Running the REPL

```bash
./build/mql              # data stored in ./data/
./build/mql ./mydb       # custom data directory
```

Data persists across restarts. Tables are stored as `.trie`, `.heap`, and `.schema` files in the data directory.

### Running the TCP server

```bash
./build/server                        # port 5432, data in ./data/
./build/server --port 9000 --data /var/db/heavy
./build/server --port 5432 --data ./data0 --cluster cluster.conf
```

The server accepts MQL commands over TCP. It automatically runs `COMPACT` on all tables after 30 seconds of idle, then waits 5 minutes before the next compaction.

#### Running a two-node cluster

Create a cluster config file that assigns each node a key range:

```
# cluster.conf — one line per node: host port lo_hex hi_hex
# "-" means open-ended (absolute min / absolute max)
127.0.0.1 5432 - 0000000080000000
127.0.0.1 5433 0000000080000000 -
```

Start each node, pointing it at the same config file:

```bash
./build/server --port 5432 --data ./data0 --cluster cluster.conf
./build/server --port 5433 --data ./data1 --cluster cluster.conf
```

Each server routes single-key ops (`NEW`, `QUERY`, `DELETE`, `UPDATE`) to the node that owns the key. `RANGE` queries are executed locally and scattered to all overlapping peers; the results are merged in key order before returning to the client. Clients can connect to any node — routing is transparent.

Range boundaries are specified as lowercase hex byte pairs in the same encoding the engine uses for the primary key type. For `number` (UINT64) columns the engine uses little-endian byte order, so the midpoint of the uint64 keyspace (`2^63`) in little-endian hex is `0000000000000080`.

#### Running primary/replica replication

Start a primary — it opens a replication log and listens for replicas on `--repl-port`:

```bash
./build/server --port 5432 --data ./primary --repl-port 5532
```

Start a replica — it connects to the primary's replication port and streams all mutations:

```bash
./build/server --port 5433 --data ./replica --primary 127.0.0.1:5532
```

The replica forwards all writes (`NEW`, `UPDATE`, `DELETE`, etc.) to the primary and serves reads locally. On reconnect after a crash, the replica resumes from its last applied LSN. The replication log (`repl.log`) uses the same CRC32-verified framing as the WAL.

### Connecting with the client

```bash
./build/client                        # connects to 127.0.0.1:5432
./build/client --host 10.0.0.1 --port 9000
```

The client is a readline REPL that sends commands to the server and prints responses.

### Running tests

```bash
cd build && ctest --output-on-failure
```

### Running benchmarks

```bash
./build/bench              # 100K records
./build/bench 500000       # 500K records

./build/ycsb               # heavy-trie YCSB, all workloads A-F, 100K records
./build/ycsb 1000000 --ops 500000 C   # workload C only, 1M records, 500K ops

# PostgreSQL comparison (requires a running Postgres and libpq-dev):
sudo service postgresql start
sudo -u postgres createuser --superuser $USER
createdb ycsb_bench
./build/ycsb_pg            # sync writes
./build/ycsb_pg --async    # SET synchronous_commit=off
```

---

## Documentation

### Heavy Trie

A binary trie stores keys bit by bit: each internal node branches on one bit, left for 0 and right for 1. A root-to-leaf traversal for an n-bit key visits n nodes — far too many disk reads for a useful index.

**Path compression** collapses runs of non-branching bits into a single edge label (the Patricia / radix trie trick), bringing depth down to the number of distinct branch points, which is at most the number of keys minus one.

**Heavy path decomposition** assigns every node a *weight* equal to the number of keys in its subtree. At each branching node the heavier child is called the *heavy child*; the other is the *light child*. Following heavy children from the root traces the *heavy path*. A light edge (crossing to a light child) at least halves the remaining subtree weight, so any root-to-leaf path crosses at most O(log n) light edges and therefore at most O(log n) heavy chains.

Each maximal consecutive run of heavy edges — a *heavy chain* — is encoded into a single 8 KB disk block together with the path bits and the addresses of any light children that branch off it. A lookup reads one block per chain crossing: **O(log n) block reads total**, identical to a B-tree.

### Storing on Disk

#### Trie file

The trie is stored in a flat file of 8 KB blocks managed by `DiskManager`. The header block (block 0) records the root address, key count, and free-list head. All other blocks are either chain blocks or pack blocks.

**Chain block** — stores the bit-path of one heavy chain plus up to 255 light-child pointers. Each entry in the chain is: `{bits, bit_count, light_child_addr, heap_offset}`. A lookup walks the chain entries sequentially, matching bits and following light children until the key is found or the chain ends.

**Pack block** — when a chain is small (under 256 bytes), it is packed with up to 26 other small chains into one block using a directory header:

```
[0..3]    magic
[4..5]    num_slots
[6..1025] directory: {data_offset(2), capacity(2)} × up to 255 slots
[1026..]  chain data — 256 bytes per slot
```

Chain addresses encode the slot: `addr = (slot_index << 56) | block_id`. Slot 0 means a dedicated block; slots 1–255 are packed positions. When a packed chain outgrows 256 bytes it is promoted to a dedicated block via a forwarding stub so existing parent pointers require no tree-wide update.

At startup, `rebuild_packed_state` scans all allocated blocks to reconstruct the in-memory packing directory. Physical block count for a 100K-record table drops from ~100,000 to ~5,700 — a **17× reduction**.

#### Heap file

Record data (the full row) is stored separately in a slotted-page heap file (`HeapFile`). The trie stores only the key and a heap offset; lookups retrieve the full row with one additional pointer dereference. Both files are mmap-backed with 4 GB virtual reservation and pre-grow in 8 MB chunks to amortise `ftruncate` calls.

### Rebalancing (flip)

When an insert causes a light subtree to outgrow its heavy sibling, the two swap roles: the old light child becomes the new heavy path and the displaced heavy tail becomes a light-child block. This *flip* restores the heavy-path invariant in O(1) extra I/Os and is triggered at most O(log n) times per insert. Orphaned blocks from flips go onto an in-memory free list persisted in the header block and are reused by subsequent allocations.

### Compaction

Over time, single inserts and flips scatter chain blocks across the file in allocation order. `COMPACT` (or the background `compact_all()` in the server) rewrites every chain in DFS pre-order:

1. A DFS pre-order traversal assigns each chain a fresh destination block.
2. Chains are copied to their destinations; the old locations get forwarding stubs.
3. A second pass follows all stubs to patch parent pointers directly.

After compaction, lookup traverses parent → heavy-child → grandchild in adjacent blocks, roughly halving I/O for cold-cache point lookups. The tradeoff is that DFS pre-order co-locates traversal-order neighbors, not lex-order neighbors, so range scan performance decreases slightly (lex-adjacent keys live in different subtrees and their chains end up in cold blocks).

In the MQL REPL or via the client:

```
COMPACT(table_name)
```

### Hot Chain Cache

`DiskTrie` maintains a 1000-entry write-back LRU cache of decoded `ChainData` objects. Reads check the cache before touching mmap memory. Writes go to the cache and increment a per-entry dirty counter; the entry is flushed to mmap when the dirty count reaches 10 or the entry is evicted. This amortises the encoding/decoding cost for hot chains at the top of the trie, which are touched on every insert.

Weight updates (needed to maintain heavy-path invariants) are further optimised: the encoded weight stored on disk is `floor(log2(subtree_count))`. At depth d, this value changes only once per ~2^d inserts, so upper-level blocks almost never need a write.

### MQL Reference

MQL (Mini Query Language) is the text protocol understood by both the REPL and the TCP server.

#### Data types

| Type | Description |
|---|---|
| `string` | Variable-length UTF-8, stored as VARCHAR |
| `number` | Unsigned 64-bit integer |

Exactly one column per table must be marked `PRIMARY KEY`.

#### Commands

| Command | Syntax | Notes |
|---|---|---|
| Create table | `TABLE name(col type [PRIMARY KEY], ...)` | Creates `.trie`, `.heap`, `.schema` files |
| Insert row | `NEW(table, val, ...)` | Returns error if PK exists |
| Bulk insert | `BULK table (val, ...) (val, ...) ...` | Suppresses per-row bookkeeping; much faster for large loads |
| Update row | `UPDATE(table, val, ...)` | Full row replacement by PK |
| Delete row | `DELETE(table, pk)` | |
| Point query | `QUERY(table, pk)` | |
| Range scan | `RANGE(table, lo, hi)` | Inclusive on both ends |
| IN lookup | `IN(table, k1, k2, ...)` | Returns matching rows in input order |
| Count | `COUNT(table)` | |
| Chain stats | `CHAINS(table)` | Active vs. allocated chain blocks |
| Compact | `COMPACT(table)` | Rewrites blocks in DFS pre-order |

#### Example session

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

COMPACT(users)

exit
```

### Server and Wire Protocol

The TCP server (`server`) accepts one MQL command per request over a simple length-prefixed text protocol:

```
Request:   "<len>\n<command>\n"
Response:  "<len>\n<result>\n"
```

`len` is the byte count of the payload, not including the trailing newline. The framing code is in [server/wire.h](server/wire.h) and is shared between server and client.

The server tracks in-flight request count via an atomic counter so the background compaction thread can safely detect an idle window. On `SIGINT` or `SIGTERM`, the server drains in-flight requests (up to 5 seconds), signals the compaction thread, and exits cleanly.

### YCSB Benchmark

The YCSB harness (`bench/ycsb.cpp`) implements all six standard workloads:

| Workload | Read | Update | Insert | Scan | RMW | Key distribution |
|---|---|---|---|---|---|---|
| A | 50% | 50% | — | — | — | Zipfian |
| B | 95% | 5% | — | — | — | Zipfian |
| C | 100% | — | — | — | — | Zipfian |
| D | 95% | — | 5% | — | — | Latest (recent-biased) |
| E | 5% | — | 5% | 95% | — | Zipfian |
| F | 50% | — | — | — | 50% | Zipfian |

Keys are generated with `make_key(id)` → `user0000000000` (14 bytes). The Zipfian generator uses θ = 0.99 with scrambling so hot items are scattered across the keyspace rather than clustered at low offsets.

`bench/ycsb_pg.cpp` drives the same workloads against PostgreSQL via libpq: bulk load via `COPY FROM STDIN` (streamed in 1 MB chunks), prepared statements for all operations, and explicit `BEGIN`/`COMMIT` batching every 100 operations.

---

## Project Structure

```
heavy-trie/
├── main.cpp                  REPL entry point
├── server/
│   ├── server.cpp            TCP server, background compaction, signal handling, cluster routing
│   ├── client.cpp            readline REPL client
│   ├── cluster.h/cpp         Peer config, key-range routing, TCP forwarding
│   ├── repl.h/cpp            Streaming replication log (primary) and client (replica)
│   └── wire.h                Shared length-prefixed framing protocol
├── bench/
│   ├── bench.cpp             Microbenchmark (BULK/INSERT/LOOKUP/RANGE/COMPACT)
│   ├── ycsb.cpp              YCSB harness for heavy-trie (workloads A–F)
│   ├── ycsb_pg.cpp           YCSB harness for PostgreSQL (libpq, same workloads)
│   └── ycsb_common.h         Shared workload defs, Zipfian generator, timing
├── index/
│   ├── chain.h/cpp           Chain encoding/decoding, path matching, slice I/O
│   └── disk_trie.h/cpp       Trie insert/lookup/delete/range + rebalancing + cache
├── storage/
│   ├── buffer_pool.h/cpp     mmap-backed pool with stripe latches
│   ├── disk_manager.h/cpp    Block allocation, free list, packed-block manager
│   ├── heap.h/cpp            Slotted-page record storage (mmap-backed)
│   └── wal.h/cpp             Logical write-ahead log for crash recovery
├── catalog/
│   └── table.h/cpp           Schema + trie + heap wired together, shared_mutex
├── mql/
│   ├── lexer.h/cpp           Tokeniser
│   └── engine.h/cpp          Parser, executor, COMPACT orchestration
├── tests/                    One test file per component
├── bench_data/               Default output directory for bench
├── ycsb_data/                Default output directory for ycsb
├── IMPROVEMENTS.md           Step-by-step optimisation history with benchmarks
└── README.md                 This file
```

---

## Future Work

### WAL performance
The engine has a logical WAL (`storage/wal.log`) that calls `fdatasync` on every `begin()` and `commit()`. This is correct but expensive — each mutation pays two kernel round-trips to storage, which dominates write latency under sequential single-threaded load. Two natural next steps: **group commit** (accumulate mutations in a buffer, fsync once per batch of N ops or every X ms — the standard PostgreSQL approach) and **async WAL** (write to the OS page cache only, accept a small window of potential loss on hard crash). The current WAL also logs at the logical (MQL command) level; a physical WAL that logs block diffs would handle partial flip corruption, which the logical WAL cannot.

### Concurrency model
The `Table` class uses a single `shared_mutex` — shared for reads, exclusive for writes. For write-heavy workloads this creates contention. A finer-grained locking scheme (per-chain or per-block latches, or an MVCC approach) would allow parallel writers.

### Server hardening
- **Thread pool** instead of unbounded thread-per-connection. Under heavy connection load the server currently spawns one OS thread per client.
- **Authentication and TLS** — currently the server accepts any connection on the configured port.
- **Client session state** — the server is stateless per-command; a session concept would allow prepared statements and multi-statement transactions.
- **Graceful client disconnect detection** — currently relies on read/write returning 0 or an error.

### Variable-length and composite keys
All keys are currently fixed-width byte strings. Supporting variable-length keys (with length-prefixed encoding in the chain) and composite primary keys (multi-column index) would make the engine usable for a wider range of workloads without a surrogate integer key.

### Transactions and multi-table operations
The engine has no cross-table atomicity. A simple transaction log and two-phase commit across tables would enable atomic inserts that span multiple tables, which is needed for foreign-key-like integrity.

### Cluster rebalancing
The current cluster implementation assigns each node a fixed key range at startup. As data distribution shifts over time, some nodes will be hotter than others. A rebalancing protocol that migrates chain blocks between nodes and updates the routing table without downtime would make the cluster self-tuning. A natural starting point is range splitting: when a node's record count exceeds a threshold, it picks a split point, migrates the upper half to a new peer, and broadcasts the updated routing config.
