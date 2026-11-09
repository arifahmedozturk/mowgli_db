#include "catalog/schema.h"
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <unistd.h>

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

// Written to a temporary file, synced, then renamed over the old one and the
// directory synced (#123): a crash during DDL leaves the old schema or the
// new one, never a truncated file the engine cannot open.
void save_schema(const std::string& path, const Schema& schema) {
    const std::string text = schema.to_text();
    const std::string tmp  = path + ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("cannot write schema: " + tmp);
    size_t done = 0;
    while (done < text.size()) {
        ssize_t r = ::write(fd, text.data() + done, text.size() - done);
        if (r <= 0) break;
        done += static_cast<size_t>(r);
    }
    bool ok = done == text.size() && ::fsync(fd) == 0;
    ok = ::close(fd) == 0 && ok;
    if (!ok || ::rename(tmp.c_str(), path.c_str()) != 0) {
        ::unlink(tmp.c_str());
        throw std::runtime_error("cannot write schema: " + path);
    }
    std::string dir = std::filesystem::path(path).parent_path().string();
    int dfd = ::open(dir.empty() ? "." : dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) { ::fsync(dfd); ::close(dfd); }
}

Schema load_schema(const std::string& path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open schema: " + path);
    std::ostringstream text;
    text << f.rdbuf();
    return Schema::parse(text.str());
}
