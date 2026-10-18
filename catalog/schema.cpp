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
//   <index count>
//   <index name> <column count> <column position>...   (one line per index)
static constexpr const char* SCHEMA_MAGIC = "mowgli-schema 1";

std::string Schema::to_text() const {
    std::ostringstream f;
    f << SCHEMA_MAGIC << "\n" << table_name << "\n" << pk_col << "\n" << cols.size() << "\n";
    for (const auto& col : cols)
        f << col.name << " " << (col.type == ColType::UINT64 ? "number" : "string") << " "
          << col.max_size << "\n";
    f << indexes.size() << "\n";
    for (const auto& ix : indexes) {
        f << ix.name << " " << ix.cols.size();
        for (size_t c : ix.cols) f << " " << c;
        f << "\n";
    }
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
    size_t index_count = 0;
    if (!(f >> index_count)) throw std::runtime_error("schema: truncated index list");
    s.indexes.resize(index_count);
    for (auto& ix : s.indexes) {
        size_t n = 0;
        if (!(f >> ix.name >> n) || n == 0) throw std::runtime_error("schema: bad index definition");
        ix.cols.resize(n);
        for (auto& c : ix.cols)
            if (!(f >> c) || c >= s.cols.size()) throw std::runtime_error("schema: bad index column");
    }
    return s;
}

const IndexDef* Schema::find_index(const std::string& name) const {
    for (const auto& ix : indexes)
        if (ix.name == name) return &ix;
    return nullptr;
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
