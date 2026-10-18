#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// A column type. Only fixed-width for now; varchar uses max_size bytes on disk.
enum class ColType { UINT64, VARCHAR };

struct ColDef {
    std::string name;
    ColType     type;
    uint16_t    max_size; // bytes: 8 for UINT64, caller-specified for VARCHAR
};

// A secondary index (#96): its entries are keyed by these columns, in order,
// followed by the primary key.
struct IndexDef {
    std::string         name;
    std::vector<size_t> cols;   // column positions in the schema
};

struct Schema {
    std::string           table_name;
    std::vector<ColDef>   cols;
    size_t                pk_col; // index of the primary key column
    std::vector<IndexDef> indexes = {};

    const IndexDef* find_index(const std::string& name) const;

    // Text form stored in <table>.schema; parse() rejects anything malformed.
    std::string   to_text() const;
    static Schema parse(const std::string& text);
};

// A row is a parallel vector of raw byte buffers, one per column.
using Row = std::vector<std::vector<uint8_t>>;

void   save_schema(const std::string& path, const Schema& schema);
Schema load_schema(const std::string& path);
