#include "catalog/table.h"
#include "catalog/row_view.h"
#include <stdexcept>

// The row format: a number is 8 bytes; a string is a 2-byte little-endian
// length followed by max_size bytes, the value padded with zeros.

std::vector<uint8_t> Table::serialize(const Row& row) const {
    if (row.size() != schema_.cols.size())
        throw std::runtime_error("row column count mismatch");

    std::vector<uint8_t> buf;
    for (size_t i = 0; i < schema_.cols.size(); i++) {
        const ColDef& col  = schema_.cols[i];
        const auto&   data = row[i];

        if (col.type == ColType::UINT64) {
            if (data.size() != 8)
                throw std::runtime_error("UINT64 column must be 8 bytes");
            buf.insert(buf.end(), data.begin(), data.end());
        } else {
            if (data.size() > col.max_size)
                throw std::runtime_error("VARCHAR value exceeds max_size");
            uint16_t len = static_cast<uint16_t>(data.size());
            buf.push_back(len & 0xFF);
            buf.push_back((len >> 8) & 0xFF);
            buf.insert(buf.end(), data.begin(), data.end());
            buf.resize(buf.size() + (col.max_size - data.size()), 0);
        }
    }
    return buf;
}

std::string Table::pk_of(const uint8_t* row, size_t size) const {
    return std::string(pk_view(row, size));
}

std::string_view Table::pk_view(const uint8_t* row, size_t size) const {
    size_t offset = 0;
    for (size_t i = 0; i < schema_.cols.size(); i++) {
        const ColDef& col = schema_.cols[i];
        if (col.type == ColType::UINT64) {
            if (i == schema_.pk_col) return std::string_view(reinterpret_cast<const char*>(row + offset), 8);
            offset += 8;
        } else {
            uint16_t len = static_cast<uint16_t>(row[offset] | (row[offset + 1] << 8));
            if (i == schema_.pk_col) return std::string_view(reinterpret_cast<const char*>(row + offset + 2), len);
            offset += 2 + col.max_size;
        }
        if (offset > size) break;
    }
    throw std::runtime_error("pk_of: malformed row");
}

Row Table::deserialize(const std::vector<uint8_t>& buf) const {
    return deserialize(buf.data(), buf.size());
}

Row Table::deserialize(const uint8_t* buf, size_t /*size*/) const {
    return RowView(schema_, buf).to_row();
}
