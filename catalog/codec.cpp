#include "catalog/codec.h"
#include <stdexcept>

std::vector<uint8_t> cell_from_text(const ColDef& col, const std::string& text) {
    if (col.type == ColType::UINT64) {
        if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
            throw std::runtime_error("expected number, got '" + text + "'");
        uint64_t v;
        try { v = std::stoull(text); }
        catch (const std::out_of_range&) { throw std::runtime_error("number out of range: " + text); }
        std::vector<uint8_t> b(8);
        for (int j = 0; j < 8; j++) b[j] = static_cast<uint8_t>(v >> ((7 - j) * 8));
        return b;
    }
    // Length against max_size is checked when a row is stored; a lookup key or
    // range bound may be longer.
    return std::vector<uint8_t>(text.begin(), text.end());
}

std::string cell_to_text(ColType type, const std::vector<uint8_t>& cell) {
    if (type == ColType::UINT64) {
        uint64_t v = 0;
        for (size_t j = 0; j < 8 && j < cell.size(); j++) v = (v << 8) | cell[j];
        return std::to_string(v);
    }
    return std::string(cell.begin(), cell.end());
}

void append_key_part(std::string& key, ColType type, const std::vector<uint8_t>& cell, bool last) {
    if (type == ColType::UINT64) {
        if (cell.size() != 8) throw std::runtime_error("key part: a number must be 8 bytes");
        key.append(reinterpret_cast<const char*>(cell.data()), 8);
        return;
    }
    if (last) { key.append(reinterpret_cast<const char*>(cell.data()), cell.size()); return; }
    for (uint8_t c : cell) {
        key.push_back(static_cast<char>(c));
        if (c == 0x00) key.push_back(static_cast<char>(0xFF));
    }
    key.push_back('\0');
    key.push_back('\0');
}

std::string encode_key(const std::vector<ColType>& types, const std::vector<std::vector<uint8_t>>& cells) {
    if (types.size() != cells.size()) throw std::runtime_error("encode_key: part count mismatch");
    std::string key;
    for (size_t i = 0; i < types.size(); i++) append_key_part(key, types[i], cells[i], i + 1 == types.size());
    return key;
}

std::vector<std::vector<uint8_t>> decode_key(const std::vector<ColType>& types, const std::string& key) {
    std::vector<std::vector<uint8_t>> cells;
    size_t pos = 0;
    for (size_t i = 0; i < types.size(); i++) {
        const bool last = i + 1 == types.size();
        std::vector<uint8_t> cell;
        if (types[i] == ColType::UINT64) {
            if (pos + 8 > key.size()) throw std::runtime_error("decode_key: truncated number");
            cell.assign(key.begin() + static_cast<ptrdiff_t>(pos), key.begin() + static_cast<ptrdiff_t>(pos + 8));
            pos += 8;
        } else if (last) {
            cell.assign(key.begin() + static_cast<ptrdiff_t>(pos), key.end());
            pos = key.size();
        } else {
            while (true) {
                if (pos >= key.size()) throw std::runtime_error("decode_key: unterminated string");
                uint8_t c = static_cast<uint8_t>(key[pos++]);
                if (c != 0x00) { cell.push_back(c); continue; }
                if (pos >= key.size()) throw std::runtime_error("decode_key: truncated escape");
                uint8_t n = static_cast<uint8_t>(key[pos++]);
                if (n == 0xFF) cell.push_back(0x00);
                else if (n == 0x00) break;
                else throw std::runtime_error("decode_key: bad escape");
            }
        }
        cells.push_back(std::move(cell));
    }
    if (pos != key.size()) throw std::runtime_error("decode_key: trailing bytes");
    return cells;
}
