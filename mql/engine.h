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
    explicit Engine(const std::string& data_dir);
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
    std::string exec_compact(std::vector<Token>& t, size_t& i);
    std::string exec_stats  ();
    std::string exec_help   ();

    static std::vector<uint8_t> encode_value(const Token& tok, ColType type);
    static std::string decode_value(const std::vector<uint8_t>& bytes, ColType type);
};
