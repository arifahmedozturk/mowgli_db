// B-tree baselines: the core of bench (bulk load, single inserts, random
// lookups, range scans, startup, bytes per key) against embedded B-trees, on
// the same keys (bench/keys.h) and 64-byte values.
//
// Both are configured like heavy-trie's Table: no fsync, no journal, reads
// through mmap. LMDB: MDB_WRITEMAP | MDB_NOSYNC | MDB_NOMETASYNC. SQLite:
// WITHOUT ROWID table (clustered B-tree on the key), synchronous=OFF,
// journal_mode=OFF, mmap_size. Single inserts are one transaction each.
//
// Usage: bench_btree [N] [data_dir] [--keys dist] [--engine lmdb|sqlite|all] [--csv file]

#include "bench/bench_common.h"
#include "bench/btree_store.h"
#include "bench/keys.h"
#include "bench/metrics.h"
#include <chrono>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>



static std::string value_for(size_t i) {
    std::string v = "v" + std::to_string(i);
    v.resize(64, '\0');
    return v;
}

// ---- workload (mirrors bench.cpp) ----

static void run_engine(const std::string& name, const std::function<std::unique_ptr<Store>()>& make,
                       const std::vector<Key>& keys, const std::string& dir, Metrics& m) {
    const size_t N = keys.size();
    std::mt19937_64 rng(42);
    std::cout << "=== " << name << " ===\n" << std::fixed << std::setprecision(2);

    // BULK LOAD: sorted keys into a fresh store, one transaction.
    std::vector<std::pair<Key, std::string>> sorted;
    sorted.reserve(N);
    for (size_t i = 0; i < N; i++) sorted.push_back({keys[i], value_for(i)});
    std::sort(sorted.begin(), sorted.end());
    std::string bulk_path = dir + "/" + name + "_bulk.db";
    double bulk_secs, startup_secs;
    uint64_t bulk_bytes;
    {
        auto s = make();
        s->open(bulk_path);
        auto t0 = Clock::now();
        s->bulk_load(sorted);
        bulk_secs  = Dur(Clock::now() - t0).count();
        bulk_bytes = s->used_bytes();
        s->close();
        auto ts = Clock::now();
        s->open(bulk_path);
        startup_secs = Dur(Clock::now() - ts).count();
    }
    sorted.clear();
    sorted.shrink_to_fit();

    // INSERT: keys in generation order, one transaction each.
    auto s = make();
    s->open(dir + "/" + name + "_insert.db");
    size_t inserted = 0;
    Latencies insert_lat;
    auto t0 = Clock::now();
    for (size_t i = 0; i < N; i++) {
        std::string v = value_for(i);
        auto ti = Clock::now();
        if (s->insert(keys[i], v)) inserted++;
        insert_lat.record(Dur(Clock::now() - ti).count());
    }
    double insert_secs = Dur(Clock::now() - t0).count();

    // LOOKUP: random order, value copied out.
    std::vector<Key> lookup_keys = keys;
    std::shuffle(lookup_keys.begin(), lookup_keys.end(), rng);
    size_t found = 0;
    Latencies lookup_lat;
    std::string v;
    auto t1 = Clock::now();
    for (const Key& k : lookup_keys) {
        auto tl = Clock::now();
        if (s->lookup(k, v)) found++;
        lookup_lat.record(Dur(Clock::now() - tl).count());
    }
    double lookup_secs = Dur(Clock::now() - t1).count();

    // RANGE: 100 random scans of ~N/1000 keys each.
    std::vector<Key> sorted_keys = keys;
    std::sort(sorted_keys.begin(), sorted_keys.end());
    size_t range_ops = std::min<size_t>(100, N / 10), range_total = 0;
    std::uniform_int_distribution<size_t> idx(0, N - 1);
    auto t2 = Clock::now();
    for (size_t r = 0; r < range_ops; r++) {
        size_t lo = idx(rng);
        range_total += s->range(sorted_keys[lo], sorted_keys[std::min(lo + N / 1000, N - 1)]);
    }
    double range_secs = Dur(Clock::now() - t2).count();

    uint64_t insert_bytes = s->used_bytes();

    std::cout << "  bulk load    : " << ops_per_sec(N, bulk_secs) / 1000 << " K ops/sec\n"
              << "  insert       : " << ops_per_sec(inserted, insert_secs) / 1000 << " K ops/sec   p50/p99/p999 "
              << insert_lat.p_us(0.50) << " / " << insert_lat.p_us(0.99) << " / " << insert_lat.p_us(0.999) << " us\n"
              << "  lookup       : " << ops_per_sec(found, lookup_secs) / 1000 << " K ops/sec   p50/p99/p999 "
              << lookup_lat.p_us(0.50) << " / " << lookup_lat.p_us(0.99) << " / " << lookup_lat.p_us(0.999) << " us"
              << "   (found " << found << " / " << N << ")\n"
              << "  range        : " << ops_per_sec(range_ops, range_secs) << " scans/sec   (" << range_total << " rows)\n"
              << "  startup      : " << startup_secs * 1000 << " ms (reopen bulk-loaded file)\n"
              << "  bytes/key    : " << double(bulk_bytes) / N << " bulk-loaded, "
              << double(insert_bytes) / N << " after single inserts\n\n";

    std::string p = name + "_";
    m.add(p + "bulk_insert",        ops_per_sec(N, bulk_secs),          "ops/s");
    m.add(p + "insert",             ops_per_sec(inserted, insert_secs), "ops/s");
    m.add_latency(p + "insert", insert_lat);
    m.add(p + "lookup",             ops_per_sec(found, lookup_secs),    "ops/s");
    m.add_latency(p + "lookup", lookup_lat);
    m.add(p + "range",              ops_per_sec(range_ops, range_secs), "scans/s");
    m.add(p + "startup_time",       startup_secs * 1000,                "ms");
    m.add(p + "bytes_per_key_bulk", double(bulk_bytes) / N,             "bytes");
    m.add(p + "bytes_per_key",      double(insert_bytes) / N,           "bytes");
}

int main(int argc, char* argv[]) {
    size_t      N        = 100'000;
    std::string data_dir = "./bench_btree_data";
    std::string key_dist = "random_u64";
    std::string engine   = "all";
    std::string csv_path;
    Metrics     m;

    for (int i = 1, pos = 0; i < argc; i++) {
        std::string a = argv[i];
        if      (a == "--csv"    && i + 1 < argc) csv_path = argv[++i];
        else if (a == "--keys"   && i + 1 < argc) key_dist = argv[++i];
        else if (a == "--engine" && i + 1 < argc) engine   = argv[++i];
        else if (pos++ == 0)                      N        = static_cast<size_t>(std::stoul(a));
        else                                      data_dir = a;
    }

    std::cout << "B-tree baselines  N=" << N << "  keys=" << key_dist
              << "  data_dir=" << data_dir << "\n\n";
    std::filesystem::remove_all(data_dir);
    std::filesystem::create_directories(data_dir);

    std::mt19937_64 rng(42);
    std::vector<Key> keys = make_keys(key_dist, N, rng);

    bool ran = false;
#ifdef HAVE_LMDB
    if (engine == "all" || engine == "lmdb") {
        run_engine("lmdb", make_lmdb_store, keys, data_dir, m);
        ran = true;
    }
#endif
#ifdef HAVE_SQLITE
    if (engine == "all" || engine == "sqlite") {
        run_engine("sqlite", make_sqlite_store, keys, data_dir, m);
        ran = true;
    }
#endif
    if (!ran) {
        std::cerr << "no engine matched '" << engine << "' (built without it?)\n";
        return 1;
    }

    m.write_csv(csv_path);
    return 0;
}
