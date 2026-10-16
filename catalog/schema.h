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

struct Schema {
    std::string          table_name;
    std::vector<ColDef>  cols;
    size_t               pk_col; // index of the primary key column

    // Text form stored in <table>.schema; parse() rejects anything malformed.
    std::string   to_text() const;
    static Schema parse(const std::string& text);
};

// A row is a parallel vector of raw byte buffers, one per column.
using Row = std::vector<std::vector<uint8_t>>;

void   save_schema(const std::string& path, const Schema& schema);
Schema load_schema(const std::string& path);
