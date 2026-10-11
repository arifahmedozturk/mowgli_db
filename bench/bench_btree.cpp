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

#ifdef HAVE_LMDB
#include <lmdb.h>
#endif
#ifdef HAVE_SQLITE
#include <sqlite3.h>
#endif

using Clock = std::chrono::steady_clock;
using Dur   = std::chrono::duration<double>;

static double ops_per_sec(size_t n, double secs) { return secs > 0 ? n / secs : 0; }

static std::string value_for(size_t i) {
    std::string v = "v" + std::to_string(i);
    v.resize(64, '\0');
    return v;
}

static bool key_le(const uint8_t* a, size_t alen, const Key& b) {
    int c = std::memcmp(a, b.data(), std::min(alen, b.size()));
    return c < 0 || (c == 0 && alen <= b.size());
}

// One embedded store under test. open() creates or reopens the file at path.
struct Store {
    virtual ~Store() = default;
    virtual void     open(const std::string& path) = 0;
    virtual void     close() = 0;
    virtual void     bulk_load(const std::vector<std::pair<Key, std::string>>& sorted) = 0;
    virtual bool     insert(const Key& k, const std::string& v) = 0;
    virtual bool     lookup(const Key& k, std::string& v) = 0;
    virtual size_t   range(const Key& lo, const Key& hi) = 0;  // rows copied out
    virtual uint64_t used_bytes() = 0;
};

// ---- LMDB ----

#ifdef HAVE_LMDB
static void lmdb_check(int rc, const char* what) {
    if (rc != MDB_SUCCESS) throw std::runtime_error(std::string("lmdb ") + what + ": " + mdb_strerror(rc));
}

class LmdbStore : public Store {
public:
    ~LmdbStore() override { close(); }

    void open(const std::string& path) override {
        lmdb_check(mdb_env_create(&env_), "env_create");
        lmdb_check(mdb_env_set_mapsize(env_, 256ULL << 30), "set_mapsize");
        lmdb_check(mdb_env_open(env_, path.c_str(),
                                MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOSYNC | MDB_NOMETASYNC, 0644),
                   "env_open");
        MDB_txn* txn;
        lmdb_check(mdb_txn_begin(env_, nullptr, 0, &txn), "txn_begin");
        lmdb_check(mdb_dbi_open(txn, nullptr, 0, &dbi_), "dbi_open");
        lmdb_check(mdb_txn_commit(txn), "txn_commit");
        lmdb_check(mdb_txn_begin(env_, nullptr, MDB_RDONLY, &rtxn_), "read txn");
        mdb_txn_reset(rtxn_);
    }

    void close() override {
        if (rtxn_) { mdb_txn_abort(rtxn_); rtxn_ = nullptr; }
        if (env_)  { mdb_env_close(env_);  env_  = nullptr; }
    }

    void bulk_load(const std::vector<std::pair<Key, std::string>>& sorted) override {
        MDB_txn* txn;
        lmdb_check(mdb_txn_begin(env_, nullptr, 0, &txn), "txn_begin");
        for (const auto& [k, v] : sorted) {
            MDB_val mk = val(k), mv = val(v);
            int rc = mdb_put(txn, dbi_, &mk, &mv, MDB_APPEND);
            if (rc != MDB_KEYEXIST) lmdb_check(rc, "put");  // duplicate random keys
        }
        lmdb_check(mdb_txn_commit(txn), "txn_commit");
    }

    bool insert(const Key& k, const std::string& v) override {
        MDB_txn* txn;
        lmdb_check(mdb_txn_begin(env_, nullptr, 0, &txn), "txn_begin");
        MDB_val mk = val(k), mv = val(v);
        int rc = mdb_put(txn, dbi_, &mk, &mv, MDB_NOOVERWRITE);
        if (rc == MDB_KEYEXIST) { mdb_txn_abort(txn); return false; }
        lmdb_check(rc, "put");
        lmdb_check(mdb_txn_commit(txn), "txn_commit");
        return true;
    }

    bool lookup(const Key& k, std::string& v) override {
        lmdb_check(mdb_txn_renew(rtxn_), "txn_renew");
        MDB_val mk = val(k), mv;
        int rc = mdb_get(rtxn_, dbi_, &mk, &mv);
        if (rc == MDB_SUCCESS) v.assign(static_cast<const char*>(mv.mv_data), mv.mv_size);
        mdb_txn_reset(rtxn_);
        return rc == MDB_SUCCESS;
    }

    size_t range(const Key& lo, const Key& hi) override {
        lmdb_check(mdb_txn_renew(rtxn_), "txn_renew");
        MDB_cursor* cur;
        lmdb_check(mdb_cursor_open(rtxn_, dbi_, &cur), "cursor_open");
        MDB_val mk = val(lo), mv;
        size_t n = 0;
        std::string k, v;
        for (int rc = mdb_cursor_get(cur, &mk, &mv, MDB_SET_RANGE);
             rc == MDB_SUCCESS && key_le(static_cast<const uint8_t*>(mk.mv_data), mk.mv_size, hi);
             rc = mdb_cursor_get(cur, &mk, &mv, MDB_NEXT)) {
            k.assign(static_cast<const char*>(mk.mv_data), mk.mv_size);
            v.assign(static_cast<const char*>(mv.mv_data), mv.mv_size);
            n++;
        }
        mdb_cursor_close(cur);
        mdb_txn_reset(rtxn_);
        return n;
    }

    uint64_t used_bytes() override {
        MDB_envinfo info;
        MDB_stat    st;
        mdb_env_info(env_, &info);
        mdb_env_stat(env_, &st);
        return (info.me_last_pgno + 1) * uint64_t(st.ms_psize);
    }

private:
    template <class T> static MDB_val val(const T& c) {
        return {c.size(), const_cast<void*>(static_cast<const void*>(c.data()))};
    }
    MDB_env* env_  = nullptr;
    MDB_txn* rtxn_ = nullptr;
    MDB_dbi  dbi_  = 0;
};
#endif

// ---- SQLite ----

#ifdef HAVE_SQLITE
class SqliteStore : public Store {
public:
    ~SqliteStore() override { close(); }

    void open(const std::string& path) override {
        if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK) throw std::runtime_error("sqlite open");
        exec("PRAGMA synchronous=OFF");
        exec("PRAGMA journal_mode=OFF");
        exec("PRAGMA mmap_size=68719476736");  // 64 GiB: read through mmap, like the trie
        exec("CREATE TABLE IF NOT EXISTS kv(k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID");
        ins_  = prepare("INSERT OR IGNORE INTO kv(k, v) VALUES(?, ?)");
        get_  = prepare("SELECT v FROM kv WHERE k = ?");
        scan_ = prepare("SELECT k, v FROM kv WHERE k >= ? AND k <= ? ORDER BY k");
    }

    void close() override {
        for (sqlite3_stmt* s : {ins_, get_, scan_}) sqlite3_finalize(s);
        ins_ = get_ = scan_ = nullptr;
        if (db_) { sqlite3_close(db_); db_ = nullptr; }
    }

    void bulk_load(const std::vector<std::pair<Key, std::string>>& sorted) override {
        exec("BEGIN");
        for (const auto& [k, v] : sorted) insert(k, v);
        exec("COMMIT");
    }

    bool insert(const Key& k, const std::string& v) override {
        bind(ins_, 1, k.data(), k.size());
        bind(ins_, 2, v.data(), v.size());
        int rc = sqlite3_step(ins_);
        sqlite3_reset(ins_);
        if (rc != SQLITE_DONE) throw std::runtime_error(std::string("sqlite insert: ") + sqlite3_errmsg(db_));
        return sqlite3_changes(db_) > 0;
    }

    bool lookup(const Key& k, std::string& v) override {
        bind(get_, 1, k.data(), k.size());
        bool found = sqlite3_step(get_) == SQLITE_ROW;
        if (found) v.assign(static_cast<const char*>(sqlite3_column_blob(get_, 0)), sqlite3_column_bytes(get_, 0));
        sqlite3_reset(get_);
        return found;
    }

    size_t range(const Key& lo, const Key& hi) override {
        bind(scan_, 1, lo.data(), lo.size());
        bind(scan_, 2, hi.data(), hi.size());
        size_t n = 0;
        std::string k, v;
        while (sqlite3_step(scan_) == SQLITE_ROW) {
            k.assign(static_cast<const char*>(sqlite3_column_blob(scan_, 0)), sqlite3_column_bytes(scan_, 0));
            v.assign(static_cast<const char*>(sqlite3_column_blob(scan_, 1)), sqlite3_column_bytes(scan_, 1));
            n++;
        }
        sqlite3_reset(scan_);
        return n;
    }

    uint64_t used_bytes() override { return pragma_int("page_count") * pragma_int("page_size"); }

private:
    void exec(const char* sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql, nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "?";
            sqlite3_free(err);
            throw std::runtime_error(std::string("sqlite: ") + sql + ": " + msg);
        }
    }
    sqlite3_stmt* prepare(const char* sql) {
        sqlite3_stmt* s = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &s, nullptr) != SQLITE_OK)
            throw std::runtime_error(std::string("sqlite prepare: ") + sqlite3_errmsg(db_));
        return s;
    }
    static void bind(sqlite3_stmt* s, int i, const void* p, size_t n) {
        sqlite3_bind_blob(s, i, p, static_cast<int>(n), SQLITE_STATIC);
    }
    uint64_t pragma_int(const char* name) {
        sqlite3_stmt* s = prepare((std::string("PRAGMA ") + name).c_str());
        uint64_t v = sqlite3_step(s) == SQLITE_ROW ? sqlite3_column_int64(s, 0) : 0;
        sqlite3_finalize(s);
        return v;
    }
    sqlite3*      db_   = nullptr;
    sqlite3_stmt* ins_  = nullptr;
    sqlite3_stmt* get_  = nullptr;
    sqlite3_stmt* scan_ = nullptr;
};
#endif

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
        run_engine("lmdb", [] { return std::make_unique<LmdbStore>(); }, keys, data_dir, m);
        ran = true;
    }
#endif
#ifdef HAVE_SQLITE
    if (engine == "all" || engine == "sqlite") {
        run_engine("sqlite", [] { return std::make_unique<SqliteStore>(); }, keys, data_dir, m);
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
