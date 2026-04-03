#pragma once
#include "catalog/table.h"
#include "mql/lexer.h"
#include <shared_mutex>
#include <string>
#include <unordered_map>

class Engine {
public:
    explicit Engine(const std::string& data_dir);
    std::string exec(const std::string& cmd);

private:
    std::string data_dir_;
    std::unordered_map<std::string, Table> tables_;
    mutable std::shared_mutex tables_latch_; // shared=read ops, exclusive=TABLE create

    std::string exec_table (std::vector<Token>& t, size_t& i);
    std::string exec_new   (std::vector<Token>& t, size_t& i);
    std::string exec_update(std::vector<Token>& t, size_t& i);
    std::string exec_query (std::vector<Token>& t, size_t& i);
    std::string exec_delete(std::vector<Token>& t, size_t& i);
    std::string exec_chains(std::vector<Token>& t, size_t& i);
    std::string exec_count (std::vector<Token>& t, size_t& i);
    std::string exec_range (std::vector<Token>& t, size_t& i);
    std::string exec_in    (std::vector<Token>& t, size_t& i);
    std::string exec_bulk  (std::vector<Token>& t, size_t& i);
    std::string exec_help  ();

    static std::vector<uint8_t> encode_value(const Token& tok, ColType type);
    static std::string decode_value(const std::vector<uint8_t>& bytes, ColType type);
};
