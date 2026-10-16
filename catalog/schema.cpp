#include "catalog/schema.h"
#include <fstream>
#include <sstream>
#include <stdexcept>

// Layout, one item per line:
//   mowgli-schema 1
//   <table name>
//   <primary key column index>
//   <column count>
//   <name> <number|string> <max_size>     (one line per column)
static constexpr const char* SCHEMA_MAGIC = "mowgli-schema 1";

std::string Schema::to_text() const {
    std::ostringstream f;
    f << SCHEMA_MAGIC << "\n" << table_name << "\n" << pk_col << "\n" << cols.size() << "\n";
    for (const auto& col : cols)
        f << col.name << " " << (col.type == ColType::UINT64 ? "number" : "string") << " "
          << col.max_size << "\n";
    return f.str();
}

Schema Schema::parse(const std::string& text) {
    std::istringstream f(text);
    std::string magic;
    std::getline(f, magic);
    if (magic != SCHEMA_MAGIC) throw std::runtime_error("schema: unknown format '" + magic + "'");
    Schema s;
    size_t col_count = 0;
    if (!(f >> s.table_name >> s.pk_col >> col_count)) throw std::runtime_error("schema: truncated header");
    s.cols.resize(col_count);
    for (auto& col : s.cols) {
        std::string type;
        if (!(f >> col.name >> type >> col.max_size)) throw std::runtime_error("schema: truncated column list");
        if      (type == "number") col.type = ColType::UINT64;
        else if (type == "string") col.type = ColType::VARCHAR;
        else throw std::runtime_error("schema: unknown column type '" + type + "'");
    }
    if (s.cols.empty() || s.pk_col >= s.cols.size()) throw std::runtime_error("schema: bad primary key column");
    return s;
}

void save_schema(const std::string& path, const Schema& schema) {
    std::ofstream f(path);
    if (!f) throw std::runtime_error("cannot write schema: " + path);
    f << schema.to_text();
}

Schema load_schema(const std::string& path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open schema: " + path);
    std::ostringstream text;
    text << f.rdbuf();
    return Schema::parse(text.str());
}
