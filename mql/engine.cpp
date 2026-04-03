#include "mql/engine.h"
#include "mql/lexer.h"
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

static void save_schema(const std::string& data_dir, const Schema& schema) {
    std::ofstream f(data_dir + "/" + schema.table_name + ".schema");
    f << schema.table_name << "\n" << schema.pk_col << "\n" << schema.cols.size() << "\n";
    for (const auto& col : schema.cols)
        f << col.name << " "
          << (col.type == ColType::UINT64 ? "number" : "string") << " "
          << col.max_size << "\n";
}

static Schema load_schema(const std::string& path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open schema: " + path);
    Schema s;
    size_t col_count;
    f >> s.table_name >> s.pk_col >> col_count;
    s.cols.resize(col_count);
    for (auto& col : s.cols) {
        std::string type_str;
        f >> col.name >> type_str >> col.max_size;
        col.type = (type_str == "number") ? ColType::UINT64 : ColType::VARCHAR;
    }
    return s;
}

Engine::Engine(const std::string& data_dir) : data_dir_(data_dir) {
    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
        if (entry.path().extension() != ".schema") continue;
        Schema schema = load_schema(entry.path().string());
        std::string name      = schema.table_name;
        std::string trie_path = data_dir_ + "/" + name + ".trie";
        std::string heap_path = data_dir_ + "/" + name + ".heap";
        if (std::filesystem::exists(trie_path) && std::filesystem::exists(heap_path))
            tables_.emplace(name, Table::open(schema, trie_path, heap_path));
    }
}

std::string Engine::exec(const std::string& cmd) {
    auto tokens = lex(cmd);
    size_t i = 0;
    if (tokens[i].type != TokenType::KEYWORD)
        throw std::runtime_error("expected command keyword");

    const std::string& kw = tokens[i].value;
    if      (kw == "TABLE")  return exec_table (tokens, i);
    else if (kw == "NEW")    return exec_new   (tokens, i);
    else if (kw == "UPDATE") return exec_update(tokens, i);
    else if (kw == "QUERY")  return exec_query (tokens, i);
    else if (kw == "DELETE") return exec_delete(tokens, i);
    else if (kw == "CHAINS") return exec_chains(tokens, i);
    else if (kw == "COUNT")  return exec_count (tokens, i);
    else if (kw == "RANGE")  return exec_range (tokens, i);
    else if (kw == "IN")     return exec_in    (tokens, i);
    else if (kw == "BULK")   return exec_bulk  (tokens, i);
    else if (kw == "HELP")   return exec_help  ();
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
    save_schema(data_dir_, schema);
    tables_.emplace(name, Table::create(schema, trie_path, heap_path));
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
        row.push_back(encode_value(t[i++], schema.cols[col].type));
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
        row.push_back(encode_value(t[i++], schema.cols[col].type));
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

    std::vector<uint8_t> pk = encode_value(t[i++], schema.cols[schema.pk_col].type);
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

    std::vector<uint8_t> pk = encode_value(t[i++], schema.cols[schema.pk_col].type);
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

    auto lo = encode_value(t[i++], schema.cols[schema.pk_col].type);
    expect(t, i, TokenType::COMMA);
    auto hi = encode_value(t[i++], schema.cols[schema.pk_col].type);
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
        keys.push_back(encode_value(t[i++], schema.cols[schema.pk_col].type));
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
            row.push_back(encode_value(t[i++], schema.cols[col].type));
        }
        expect(t, i, TokenType::RPAREN);
        rows.push_back(std::move(row));
    }

    size_t inserted = table.bulk_insert(std::move(rows));
    return std::to_string(inserted) + " inserted";
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
        "  CHAINS (table)                            — active and allocated chain blocks\n"
        "  HELP                                      — show this message\n"
        "  exit / quit                               — exit the REPL";
}

std::vector<uint8_t> Engine::encode_value(const Token& tok, ColType type) {
    if (type == ColType::UINT64) {
        if (tok.type != TokenType::NUMBER)
            throw std::runtime_error("expected number, got '" + tok.value + "'");
        uint64_t v = std::stoull(tok.value);
        std::vector<uint8_t> b(8);
        for (int j = 0; j < 8; j++) b[j] = (v >> (j * 8)) & 0xFF;
        return b;
    } else {
        if (tok.type != TokenType::STRING)
            throw std::runtime_error("expected string, got '" + tok.value + "'");
        return std::vector<uint8_t>(tok.value.begin(), tok.value.end());
    }
}

std::string Engine::decode_value(const std::vector<uint8_t>& bytes, ColType type) {
    if (type == ColType::UINT64) {
        uint64_t v = 0;
        for (int j = 0; j < 8 && j < (int)bytes.size(); j++)
            v |= static_cast<uint64_t>(bytes[j]) << (j * 8);
        return std::to_string(v);
    } else {
        return std::string(bytes.begin(), bytes.end());
    }
}
