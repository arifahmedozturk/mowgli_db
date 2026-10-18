#include "mql/engine.h"
#include "mql/lexer.h"
#include "catalog/codec.h"
#include "catalog/schema.h"
#include <stdexcept>
#include <cstring>
#include <fstream>
#include <filesystem>
#include <shared_mutex>

static Token& expect(std::vector<Token>& t, size_t& i, TokenType type) {
    if (t[i].type != type)
        throw std::runtime_error("unexpected token: '" + t[i].value + "'");
    return t[i++];
}

static Token& expect_kw(std::vector<Token>& t, size_t& i, const std::string& kw) {
    if (t[i].type != TokenType::KEYWORD || t[i].value != kw)
        throw std::runtime_error("expected keyword '" + kw + "', got '" + t[i].value + "'");
    return t[i++];
}

Engine::Engine(const std::string& data_dir, bool wal, Wal::Sync sync)
    : data_dir_(data_dir)
    , wal_(Wal::open(data_dir + "/wal.log", sync))
    , wal_enabled_(wal)
{
    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
        if (entry.path().extension() != ".schema") continue;
        Schema schema = load_schema(entry.path().string());
        std::string name      = schema.table_name;
        std::string trie_path = data_dir_ + "/" + name + ".trie";
        std::string heap_path = data_dir_ + "/" + name + ".heap";
        if (std::filesystem::exists(trie_path) && std::filesystem::exists(heap_path))
            tables_.emplace(name, Table::open(schema, trie_path, heap_path)).first->second.set_durable(true);
    }

    // Replay any mutations that were written to the WAL but not committed
    // before the last crash. wal_on_ is false so replayed commands don't
    // get re-logged.
    auto pending = wal_.recover();
    if (!pending.empty()) {
        wal_on_ = false;
        for (const auto& cmd : pending) {
            try { exec(cmd); } catch (...) {}
        }
    }
    wal_on_ = wal_enabled_;
    wal_.truncate();
}

void Engine::compact_all() {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    for (auto& [name, tbl] : tables_)
        tbl.compact();
}

std::string Engine::exec(const std::string& cmd) {
    auto tokens = lex(cmd);
    size_t i = 0;
    if (tokens[i].type != TokenType::KEYWORD)
        throw std::runtime_error("expected command keyword");

    const std::string& kw = tokens[i].value;

    if (is_mutation(kw)) return exec_mutation(cmd, tokens, i);

    if      (kw == "QUERY")   return exec_query  (tokens, i);
    else if (kw == "CHAINS")  return exec_chains (tokens, i);
    else if (kw == "COUNT")   return exec_count  (tokens, i);
    else if (kw == "RANGE")   return exec_range  (tokens, i);
    else if (kw == "IN")      return exec_in     (tokens, i);
    else if (kw == "COMPACT") return exec_compact(tokens, i);
    else if (kw == "STATS")   return exec_stats  ();
    else if (kw == "HELP")    return exec_help   ();
    throw std::runtime_error("unknown command: " + kw);
}

std::string Engine::exec_table(std::vector<Token>& t, size_t& i) {
    i++;
    std::string name = expect(t, i, TokenType::IDENT).value;
    expect(t, i, TokenType::LPAREN);

    Schema schema;
    schema.table_name = name;
    schema.pk_col     = SIZE_MAX;

    while (t[i].type != TokenType::RPAREN) {
        if (t[i].type == TokenType::COMMA) { i++; continue; }

        std::string col_name = expect(t, i, TokenType::IDENT).value;

        if (t[i].type != TokenType::IDENT)
            throw std::runtime_error("expected type for column '" + col_name + "'");
        std::string type_str = t[i++].value;
        std::string upper = type_str;
        for (char& c : upper) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

        ColType ctype;
        uint16_t max_size;
        if (upper == "STRING") {
            ctype    = ColType::VARCHAR;
            max_size = 64;
        } else if (upper == "NUMBER") {
            ctype    = ColType::UINT64;
            max_size = 8;
        } else {
            throw std::runtime_error("unknown type: " + type_str);
        }

        bool is_pk = false;
        if (t[i].type == TokenType::KEYWORD && t[i].value == "PRIMARY") {
            i++;
            expect_kw(t, i, "KEY");
            is_pk = true;
        }

        if (is_pk) schema.pk_col = schema.cols.size();
        schema.cols.push_back({col_name, ctype, max_size});
    }
    i++;

    if (schema.pk_col == SIZE_MAX)
        throw std::runtime_error("TABLE '" + name + "' has no PRIMARY KEY");

    std::unique_lock<std::shared_mutex> lock(tables_latch_);
    if (tables_.count(name))
        throw std::runtime_error("table '" + name + "' already exists");

    std::string trie_path = data_dir_ + "/" + name + ".trie";
    std::string heap_path = data_dir_ + "/" + name + ".heap";
    save_schema(data_dir_ + "/" + name + ".schema", schema);
    tables_.emplace(name, Table::create(schema, trie_path, heap_path)).first->second.set_durable(true);
    return "OK";
}

std::string Engine::exec_new(std::vector<Token>& t, size_t& i) {
    i++;
    expect(t, i, TokenType::LPAREN);
    std::string name = expect(t, i, TokenType::IDENT).value;
    expect(t, i, TokenType::COMMA);

    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(name);
    if (it == tables_.end())
        throw std::runtime_error("unknown table: " + name);
    Table& table = it->second;
    const Schema& schema = table.schema();

    Row row;
    for (size_t col = 0; col < schema.cols.size(); col++) {
        if (col > 0) expect(t, i, TokenType::COMMA);
        row.push_back(encode_value(t[i++], schema.cols[col]));
    }
    expect(t, i, TokenType::RPAREN);

    if (!table.insert(row))
        return "DUPLICATE KEY";
    return "OK";
}

std::string Engine::exec_update(std::vector<Token>& t, size_t& i) {
    i++;
    expect(t, i, TokenType::LPAREN);
    std::string name = expect(t, i, TokenType::IDENT).value;
    expect(t, i, TokenType::COMMA);

    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(name);
    if (it == tables_.end())
        throw std::runtime_error("unknown table: " + name);
    Table& table = it->second;
    const Schema& schema = table.schema();

    Row row;
    for (size_t col = 0; col < schema.cols.size(); col++) {
        if (col > 0) expect(t, i, TokenType::COMMA);
        row.push_back(encode_value(t[i++], schema.cols[col]));
    }
    expect(t, i, TokenType::RPAREN);

    if (!table.update(row))
        return "NOT FOUND";
    return "OK";
}

std::string Engine::exec_query(std::vector<Token>& t, size_t& i) {
    i++;
    expect(t, i, TokenType::LPAREN);
    std::string name = expect(t, i, TokenType::IDENT).value;
    expect(t, i, TokenType::COMMA);

    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(name);
    if (it == tables_.end())
        throw std::runtime_error("unknown table: " + name);
    Table& table = it->second;
    const Schema& schema = table.schema();

    std::vector<uint8_t> pk = encode_value(t[i++], schema.cols[schema.pk_col]);
    expect(t, i, TokenType::RPAREN);

    Row row;
    size_t chains = 0;
    if (!table.lookup(pk, &row, &chains)) return "NOT FOUND";

    std::string result;
    for (size_t col = 0; col < schema.cols.size(); col++) {
        if (col > 0) result += " | ";
        result += decode_value(row[col], schema.cols[col].type);
    }
    result += "  [" + std::to_string(chains) + " chains]";
    return result;
}

std::string Engine::exec_chains(std::vector<Token>& t, size_t& i) {
    i++;
    expect(t, i, TokenType::LPAREN);
    std::string name = expect(t, i, TokenType::IDENT).value;
    expect(t, i, TokenType::RPAREN);

    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(name);
    if (it == tables_.end())
        throw std::runtime_error("unknown table: " + name);

    Table& tbl = it->second;
    return std::to_string(tbl.active_chain_count()) + " active, "
         + std::to_string(tbl.chain_count())        + " allocated";
}

std::string Engine::exec_delete(std::vector<Token>& t, size_t& i) {
    i++;
    expect(t, i, TokenType::LPAREN);
    std::string name = expect(t, i, TokenType::IDENT).value;
    expect(t, i, TokenType::COMMA);

    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(name);
    if (it == tables_.end())
        throw std::runtime_error("unknown table: " + name);
    Table& table = it->second;
    const Schema& schema = table.schema();

    std::vector<uint8_t> pk = encode_value(t[i++], schema.cols[schema.pk_col]);
    expect(t, i, TokenType::RPAREN);

    return table.remove(pk) ? "OK" : "NOT FOUND";
}

std::string Engine::exec_count(std::vector<Token>& t, size_t& i) {
    i++;
    expect(t, i, TokenType::LPAREN);
    std::string name = expect(t, i, TokenType::IDENT).value;
    expect(t, i, TokenType::RPAREN);

    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(name);
    if (it == tables_.end())
        throw std::runtime_error("unknown table: " + name);

    return std::to_string(it->second.record_count()) + " records";
}

std::string Engine::exec_range(std::vector<Token>& t, size_t& i) {
    i++;
    expect(t, i, TokenType::LPAREN);
    std::string name = expect(t, i, TokenType::IDENT).value;
    expect(t, i, TokenType::COMMA);

    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(name);
    if (it == tables_.end()) throw std::runtime_error("unknown table: " + name);
    Table& table = it->second;
    const Schema& schema = table.schema();

    auto lo = encode_value(t[i++], schema.cols[schema.pk_col]);
    expect(t, i, TokenType::COMMA);
    auto hi = encode_value(t[i++], schema.cols[schema.pk_col]);
    expect(t, i, TokenType::RPAREN);

    auto rows = table.range(lo, hi);
    if (rows.empty()) return "NOT FOUND";

    std::string result;
    for (const auto& row : rows) {
        for (size_t col = 0; col < schema.cols.size(); col++) {
            if (col > 0) result += " | ";
            result += decode_value(row[col], schema.cols[col].type);
        }
        result += "\n";
    }
    result += "(" + std::to_string(rows.size()) + " rows)";
    return result;
}

std::string Engine::exec_in(std::vector<Token>& t, size_t& i) {
    i++;
    expect(t, i, TokenType::LPAREN);
    std::string name = expect(t, i, TokenType::IDENT).value;
    expect(t, i, TokenType::COMMA);

    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(name);
    if (it == tables_.end()) throw std::runtime_error("unknown table: " + name);
    Table& table = it->second;
    const Schema& schema = table.schema();

    std::vector<std::vector<uint8_t>> keys;
    while (t[i].type != TokenType::RPAREN) {
        if (t[i].type == TokenType::COMMA) { i++; continue; }
        keys.push_back(encode_value(t[i++], schema.cols[schema.pk_col]));
    }
    i++;

    auto rows = table.in_lookup(keys);
    if (rows.empty()) return "NOT FOUND";

    std::string result;
    for (const auto& row : rows) {
        for (size_t col = 0; col < schema.cols.size(); col++) {
            if (col > 0) result += " | ";
            result += decode_value(row[col], schema.cols[col].type);
        }
        result += "\n";
    }
    result += "(" + std::to_string(rows.size()) + " rows)";
    return result;
}

std::string Engine::exec_bulk(std::vector<Token>& t, size_t& i) {
    i++;
    std::string name = expect(t, i, TokenType::IDENT).value;

    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(name);
    if (it == tables_.end())
        throw std::runtime_error("unknown table: " + name);
    Table& table = it->second;
    const Schema& schema = table.schema();

    std::vector<Row> rows;
    while (t[i].type == TokenType::LPAREN) {
        i++;
        Row row;
        for (size_t col = 0; col < schema.cols.size(); col++) {
            if (col > 0) expect(t, i, TokenType::COMMA);
            row.push_back(encode_value(t[i++], schema.cols[col]));
        }
        expect(t, i, TokenType::RPAREN);
        rows.push_back(std::move(row));
    }

    size_t inserted = table.bulk_insert(std::move(rows));
    return std::to_string(inserted) + " inserted";
}

std::string Engine::exec_compact(std::vector<Token>& t, size_t& i) {
    i++;
    expect(t, i, TokenType::LPAREN);
    std::string name = expect(t, i, TokenType::IDENT).value;
    expect(t, i, TokenType::RPAREN);

    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(name);
    if (it == tables_.end())
        throw std::runtime_error("unknown table: " + name);
    it->second.compact();
    return "OK";
}

std::string Engine::exec_stats() {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);

    std::string out;
    out += "inflight: " + std::to_string(inflight.load(std::memory_order_relaxed)) + "\n";

    for (auto& [name, table] : tables_) {
        uint64_t hits   = table.cache_hits();
        uint64_t misses = table.cache_misses();
        uint64_t total  = hits + misses;
        std::string hit_pct = total > 0
            ? std::to_string(hits * 100 / total) + "%"
            : "n/a";

        out += name + ": "
            + std::to_string(table.record_count()) + " keys"
            + "  cache " + std::to_string(hits) + "/" + std::to_string(total)
            + " (" + hit_pct + " hit)\n";
    }

    if (!out.empty() && out.back() == '\n') out.pop_back();
    return out;
}

std::string Engine::exec_help() {
    return
        "Commands:\n"
        "  TABLE  name(col type [PRIMARY KEY], ...)  — create a table\n"
        "                                              types: string, number\n"
        "  NEW    (table, val, ...)                  — insert a new row\n"
        "  BULK   table (v,...) (v,...) ...          — batch insert, no per-row rebalancing\n"
        "  UPDATE (table, val, ...)                  — overwrite existing row by primary key\n"
        "  QUERY  (table, pk)                        — lookup row by primary key\n"
        "  DELETE (table, pk)                        — delete row by primary key\n"
        "  RANGE  (table, lo, hi)                    — all rows with lo <= pk <= hi\n"
        "  IN     (table, k1, k2, ...)               — fetch a specific set of keys\n"
        "  COUNT  (table)                            — number of records\n"
        "  CHAINS  (table)                           — active and allocated chain blocks\n"
        "  COMPACT (table)                           — repack chains for better read locality\n"
        "  STATS                                     — in-flight requests + per-table cache stats\n"
        "  HELP                                      — show this message\n"
        "  exit / quit                               — exit the REPL";
}

std::string Engine::pk_bytes_for_routing(const std::string& cmd) const {
    try {
        auto tokens = lex(cmd);
        size_t i = 0;
        if (tokens[i].type != TokenType::KEYWORD) return {};
        const std::string& kw = tokens[i].value;
        bool is_new_or_update = (kw == "NEW" || kw == "UPDATE");
        if (!is_new_or_update && kw != "QUERY" && kw != "DELETE") return {};
        i++;
        if (tokens[i].type != TokenType::LPAREN) return {};
        i++;
        if (tokens[i].type != TokenType::IDENT) return {};
        std::string name = tokens[i++].value;

        std::shared_lock<std::shared_mutex> lock(tables_latch_);
        auto it = tables_.find(name);
        if (it == tables_.end()) return {};
        const Schema& schema = it->second.schema();

        if (is_new_or_update) {
            for (size_t col = 0; col < schema.cols.size(); col++) {
                if (tokens[i].type != TokenType::COMMA) return {};
                i++;
                if (col == schema.pk_col) {
                    auto bytes = encode_value(tokens[i], schema.cols[col]);
                    return std::string(bytes.begin(), bytes.end());
                }
                i++;
            }
            return {};
        } else {
            if (tokens[i].type != TokenType::COMMA) return {};
            i++;
            auto bytes = encode_value(tokens[i], schema.cols[schema.pk_col]);
            return std::string(bytes.begin(), bytes.end());
        }
    } catch (...) {
        return {};
    }
}

bool Engine::range_bytes_for_routing(const std::string& cmd,
                                      std::string& lo, std::string& hi) const {
    try {
        auto tokens = lex(cmd);
        size_t i = 0;
        if (tokens[i].type != TokenType::KEYWORD || tokens[i].value != "RANGE") return false;
        i++;
        if (tokens[i].type != TokenType::LPAREN) return false;
        i++;
        if (tokens[i].type != TokenType::IDENT) return false;
        std::string name = tokens[i++].value;

        std::shared_lock<std::shared_mutex> lock(tables_latch_);
        auto it = tables_.find(name);
        if (it == tables_.end()) return false;
        const Schema& schema = it->second.schema();
        const ColDef& pk_col = schema.cols[schema.pk_col];

        if (tokens[i].type != TokenType::COMMA) return false;
        i++;
        auto lo_bytes = encode_value(tokens[i++], pk_col);
        if (tokens[i].type != TokenType::COMMA) return false;
        i++;
        auto hi_bytes = encode_value(tokens[i], pk_col);

        lo = std::string(lo_bytes.begin(), lo_bytes.end());
        hi = std::string(hi_bytes.begin(), hi_bytes.end());
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<uint8_t> Engine::encode_value(const Token& tok, const ColDef& col) {
    if (col.type == ColType::UINT64 && tok.type != TokenType::NUMBER)
        throw std::runtime_error("expected number, got '" + tok.value + "'");
    if (col.type == ColType::VARCHAR && tok.type != TokenType::STRING)
        throw std::runtime_error("expected string, got '" + tok.value + "'");
    return cell_from_text(col, tok.value);
}

std::string Engine::decode_value(const std::vector<uint8_t>& bytes, ColType type) {
    return cell_to_text(type, bytes);
}
