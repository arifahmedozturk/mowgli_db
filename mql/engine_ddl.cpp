#include "mql/engine.h"
#include "catalog/schema.h"
#include <algorithm>
#include <filesystem>
#include <shared_mutex>
#include <stdexcept>

// CREATE TABLE, DROP TABLE (#86), CREATE INDEX and DROP INDEX (#97), SHOW
// TABLES and DESCRIBE.

static constexpr size_t DEFAULT_STRING_SIZE = 64;
static constexpr size_t MAX_STRING_SIZE     = 1024;

// A column's type as written: NUMBER / INT / BIGINT, or STRING / TEXT / VARCHAR[(n)].
static ColDef column_def(const ColumnSpec& c) {
    const std::string& t = c.type;
    if (t == "NUMBER" || t == "INT" || t == "BIGINT") {
        if (c.size) throw std::runtime_error("type " + t + " takes no size");
        return {c.name, ColType::UINT64, 8};
    }
    if (t == "STRING" || t == "TEXT" || t == "VARCHAR") {
        size_t n = c.size ? c.size : DEFAULT_STRING_SIZE;
        if (n > MAX_STRING_SIZE)
            throw std::runtime_error("column " + c.name + ": at most " + std::to_string(MAX_STRING_SIZE) + " bytes");
        return {c.name, ColType::VARCHAR, static_cast<uint16_t>(n)};
    }
    throw std::runtime_error("unknown type: " + t);
}

std::string Engine::run_create(const CreateTable& c) {
    Schema schema;
    schema.table_name = c.table;
    schema.pk_col     = SIZE_MAX;
    for (const ColumnSpec& spec : c.columns) {
        for (const ColDef& prev : schema.cols)
            if (prev.name == spec.name) throw std::runtime_error("duplicate column: " + spec.name);
        if (spec.primary_key) {
            if (schema.pk_col != SIZE_MAX) throw std::runtime_error("table " + c.table + " has two PRIMARY KEYs");
            schema.pk_col = schema.cols.size();
        }
        schema.cols.push_back(column_def(spec));
    }
    if (schema.pk_col == SIZE_MAX)
        throw std::runtime_error("table " + c.table + " has no PRIMARY KEY");

    std::unique_lock<std::shared_mutex> lock(tables_latch_);
    if (tables_.count(c.table))
        throw std::runtime_error("table '" + c.table + "' already exists");
    std::string trie_path = data_dir_ + "/" + c.table + ".trie";
    std::string heap_path = data_dir_ + "/" + c.table + ".heap";
    save_schema(data_dir_ + "/" + c.table + ".schema", schema);
    tables_.emplace(c.table, Table::create(schema, trie_path, heap_path)).first->second.set_durable(true);
    return "OK";
}

std::string Engine::run_drop(const DropTable& d) {
    std::unique_lock<std::shared_mutex> lock(tables_latch_);
    auto it = tables_.find(d.table);
    if (it == tables_.end()) throw std::runtime_error("unknown table: " + d.table);
    tables_.erase(it);   // closes its files
    // Every file of the table, its indexes and their Bloom filters is named
    // "<table>.<...>"; table names have no '.', so no other table's are.
    const std::string prefix = d.table + ".";
    for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
        const std::string f = entry.path().filename().string();
        if (f.compare(0, prefix.size(), prefix) == 0) std::filesystem::remove(entry.path());
    }
    return "OK";
}

// The table builds the index over its rows (Table::create_index); the
// schema file then records it, so the table opens with it next time. Both
// hold tables_latch_ exclusively: the planner reads a table's index list
// without the table's own latch.
std::string Engine::run_create_index(const CreateIndex& c) {
    std::unique_lock<std::shared_mutex> lock(tables_latch_);
    Table& t = table(c.table);
    IndexDef def{c.index, {}};
    for (const auto& col : c.columns) {
        size_t i = column(t.schema(), col);
        for (size_t prev : def.cols)
            if (prev == i) throw std::runtime_error("column " + col + " twice in index " + c.index);
        def.cols.push_back(i);
    }
    t.create_index(def);
    save_schema(data_dir_ + "/" + c.table + ".schema", t.schema());
    return "OK";
}

std::string Engine::run_drop_index(const DropIndex& d) {
    std::unique_lock<std::shared_mutex> lock(tables_latch_);
    Table& t = table(d.table);
    t.drop_index(d.index);
    save_schema(data_dir_ + "/" + d.table + ".schema", t.schema());
    return "OK";
}

std::string Engine::run_show_tables() {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    std::vector<std::string> names;
    for (const auto& [name, t] : tables_) names.push_back(name);
    std::sort(names.begin(), names.end());
    std::string out;
    for (const auto& n : names) out += n + "\n";
    return out + "(" + std::to_string(names.size()) + " tables)";
}

std::string Engine::run_describe(const Describe& d) {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    const Schema& s = table(d.table).schema();
    std::string out;
    for (size_t i = 0; i < s.cols.size(); i++) {
        const ColDef& c = s.cols[i];
        out += c.name + " " + (c.type == ColType::UINT64 ? "NUMBER" : "VARCHAR(" + std::to_string(c.max_size) + ")");
        if (i == s.pk_col) out += " PRIMARY KEY";
        out += "\n";
    }
    for (const IndexDef& ix : s.indexes) {
        out += "INDEX " + ix.name + " (";
        for (size_t k = 0; k < ix.cols.size(); k++) out += (k ? ", " : "") + s.cols[ix.cols[k]].name;
        out += ")\n";
    }
    if (!out.empty()) out.pop_back();
    return out;
}
