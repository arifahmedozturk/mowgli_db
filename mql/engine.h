#pragma once
#include "catalog/table.h"
#include "mql/lexer.h"
#include "storage/wal.h"
#include <atomic>
#include <shared_mutex>
#include <string>
#include <unordered_map>

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

    // Returns raw pk bytes if cmd is a single-key op (NEW/QUERY/DELETE/UPDATE),
    // empty string otherwise. Used by the server for cluster routing.
    std::string pk_bytes_for_routing(const std::string& cmd) const;

    // Fills lo/hi with raw pk bytes and returns true if cmd is a RANGE op.
    bool range_bytes_for_routing(const std::string& cmd,
                                  std::string& lo, std::string& hi) const;

    // Inflight request counter — server increments before exec(), decrements after.
    std::atomic<int> inflight{0};

private:
    std::string data_dir_;
    std::unordered_map<std::string, Table> tables_;
    mutable std::shared_mutex tables_latch_; // shared=read ops, exclusive=TABLE create
    Wal  wal_;
    bool wal_enabled_;
    bool wal_on_ = true; // disabled during crash recovery to avoid double-logging

    std::string exec_table (std::vector<Token>& t, size_t& i);
    std::string exec_new   (std::vector<Token>& t, size_t& i);
    std::string exec_update(std::vector<Token>& t, size_t& i);
    std::string exec_query (std::vector<Token>& t, size_t& i);
    std::string exec_delete(std::vector<Token>& t, size_t& i);
    std::string exec_chains(std::vector<Token>& t, size_t& i);
    std::string exec_count (std::vector<Token>& t, size_t& i);
    std::string exec_range (std::vector<Token>& t, size_t& i);
    std::string exec_in    (std::vector<Token>& t, size_t& i);
    std::string exec_bulk   (std::vector<Token>& t, size_t& i);
    // A mutation, logged and committed (engine_mutate.cpp).
    static bool is_mutation(const std::string& kw);
    std::string exec_mutation(const std::string& cmd, std::vector<Token>& t, size_t& i);
    void        commit_tables();   // engine_commit.cpp (#28)
    std::string exec_compact(std::vector<Token>& t, size_t& i);
    std::string exec_stats  ();
    std::string exec_help   ();

    static std::vector<uint8_t> encode_value(const Token& tok, const ColDef& col);
    static std::string decode_value(const std::vector<uint8_t>& bytes, ColType type);
};
