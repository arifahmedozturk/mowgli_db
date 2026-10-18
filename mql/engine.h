#pragma once
#include "catalog/table.h"
#include "mql/ast.h"
#include "storage/wal.h"
#include <atomic>
#include <functional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

// Runs statements of the SQL-like query language (mql/ast.h) against the
// tables in a data directory. By file: opening, recovery and dispatch here;
// writes logged to the WAL (engine_mutate.cpp); CREATE / DROP / SHOW /
// DESCRIBE (engine_ddl.cpp); INSERT / UPDATE / DELETE (engine_write.cpp);
// SELECT and the admin statements (engine_read.cpp); turning a WHERE clause
// into a key lookup, key range or scan (engine_plan.cpp); cluster routing
// (engine_route.cpp).
class Engine {
public:
    // wal=false skips logging new mutations (pending WAL entries are still replayed).
    // sync: whether a logged mutation waits for its WAL records to reach disk
    // (Wal::Sync::COMMIT, shared by concurrent writers) or only survives the
    // process crashing (Wal::Sync::NONE); see storage/wal.h.
    explicit Engine(const std::string& data_dir, bool wal = true, Wal::Sync sync = Wal::Sync::COMMIT);
    std::string exec(const std::string& cmd);

    // Compact all tables. Safe to call from a background thread.
    void compact_all();

    // Where a command's rows live, for cluster routing (#92): at one primary
    // key (an INSERT of one row, or a WHERE fixing the key), in a key range
    // (a WHERE bounding the key), or anywhere. write: the command changes
    // data, so a replica forwards it to the primary, which replicates it.
    struct Route {
        enum class Kind { ANY, KEY, RANGE } kind = Kind::ANY;
        std::string key, lo, hi;   // encoded primary-key bytes
        bool        write = false;
    };
    // Throws on a syntax error.
    Route route(const std::string& cmd) const;

    // Inflight request counter — server increments before exec(), decrements after.
    std::atomic<int> inflight{0};

    // How a WHERE clause is answered (engine_plan.cpp): one key, a key range
    // [lo, hi], or every row; every condition is still checked on each row.
    struct Plan {
        // NONE: nothing can match; KEYS: the keys of an IN list on the key, sorted.
        enum class Kind { NONE, KEY, KEYS, RANGE, SCAN } kind = Kind::SCAN;
        std::vector<uint8_t>              key, lo, hi;
        std::vector<std::vector<uint8_t>> keys;
        // KEY / KEYS: every condition is on the key and is =, IN or an
        // inclusive bound, so a stored key in the plan matches without
        // checking the row.
        bool exact = false;
    };
    // The keys of an exact KEY / KEYS plan (else nullptr): a statement that
    // needs only the keys can skip reading the rows.
    static const std::vector<std::vector<uint8_t>>* exact_keys(Plan& p);
    static Plan plan(const Schema& schema, const Where& where);

private:
    std::string data_dir_;
    std::unordered_map<std::string, Table> tables_;
    mutable std::shared_mutex tables_latch_; // shared: statements on rows; exclusive: CREATE / DROP TABLE
    Wal  wal_;
    bool wal_enabled_;
    bool wal_on_ = true; // disabled during crash recovery to avoid double-logging

    // The named table; the caller holds tables_latch_. Throws if unknown.
    Table&       table(const std::string& name);
    const Table& table(const std::string& name) const;

    std::string run(const Statement& s);
    std::string exec_mutation(const std::string& cmd, const Statement& s);   // engine_mutate.cpp
    void        commit_tables();                                             // engine_commit.cpp (#28)

    std::string run_create(const CreateTable& c);   // engine_ddl.cpp
    std::string run_drop(const DropTable& d);
    std::string run_show_tables();
    std::string run_describe(const Describe& d);

    std::string run_insert(const Insert& s);   // engine_write.cpp
    std::string run_update(const Update& s);
    std::string run_delete(const Delete& s);

    std::string run_select(const Select& s);   // engine_read.cpp
    std::string run_compact(const Compact& s);
    std::string run_chains(const Chains& s);
    std::string run_stats();
    static std::string help_text();

public:
    // Rows matching where, in key order, until fn returns false (engine_plan.cpp).
    static void scan(Table& t, const Where& where, const std::function<bool(const Row&)>& fn);
    // A literal as a column's bytes; throws if its kind does not fit the column.
    static std::vector<uint8_t> encode(const Literal& v, const ColDef& col);
    static size_t column(const Schema& schema, const std::string& name);   // throws if unknown
};
