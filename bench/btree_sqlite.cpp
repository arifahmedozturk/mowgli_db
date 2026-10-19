// SQLite as a bench_btree store: a WITHOUT ROWID table (clustered B-tree on
// the key), synchronous=OFF, journal_mode=OFF, mmap_size.
#include "bench/btree_store.h"
#include <sqlite3.h>
#include <stdexcept>

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

std::unique_ptr<Store> make_sqlite_store() { return std::make_unique<SqliteStore>(); }
