#pragma once
// A serialized row seen where it lies (#64): each column's bytes without
// copying. Valid only while the bytes are, e.g. during a range_view callback.
//
// Row layout (Table::serialize): a number is 8 bytes; a string is a 2-byte
// little-endian length followed by max_size bytes, the value padded with zeros.
#include "catalog/schema.h"
#include <cstddef>
#include <cstdint>
#include <utility>

class RowView {
public:
    RowView(const Schema& schema, const uint8_t* data) : schema_(schema), data_(data) {}

    size_t columns() const { return schema_.cols.size(); }

    // Column i's value bytes.
    std::pair<const uint8_t*, size_t> cell(size_t i) const {
        size_t off = 0;
        for (size_t c = 0; c < i; c++) off += width(schema_.cols[c]);
        if (schema_.cols[i].type == ColType::UINT64) return {data_ + off, 8};
        size_t len = size_t(data_[off]) | size_t(data_[off + 1]) << 8;
        return {data_ + off + 2, len};
    }

    Row to_row() const {
        Row row;
        row.reserve(columns());
        for (size_t i = 0; i < columns(); i++) {
            auto [p, n] = cell(i);
            row.emplace_back(p, p + n);
        }
        return row;
    }

    // Bytes a column takes in the row.
    static size_t width(const ColDef& col) { return col.type == ColType::UINT64 ? 8 : 2 + size_t(col.max_size); }

private:
    const Schema&  schema_;
    const uint8_t* data_;
};
