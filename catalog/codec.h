#pragma once
#include "catalog/schema.h"
#include <cstdint>
#include <string>
#include <vector>

// Conversions between column values and the bytes stored in rows and keys.
//
// Cell bytes are what a Row holds: a number is 8 bytes big-endian (so the
// trie's bytewise order is numeric order), a string is its raw bytes.

// A literal from a query, as text, to cell bytes. Throws if a number does not
// parse.
std::vector<uint8_t> cell_from_text(const ColDef& col, const std::string& text);
// Cell bytes back to text for output.
std::string          cell_to_text(ColType type, const std::vector<uint8_t>& cell);

// ---- composite keys ----
// Several cells joined into one key whose bytewise order is the order of the
// tuples (compared cell by cell), as secondary indexes need: (value, primary
// key) must sort by value first, and one tuple's bytes must never be a prefix
// that reorders it against another. Numbers are fixed-width so they need no
// framing. A string is escaped (0x00 -> 00 FF) and ends with 00 00, except as
// the last part, which is stored raw.
void                 append_key_part(std::string& key, ColType type, const std::vector<uint8_t>& cell, bool last);
std::string          encode_key(const std::vector<ColType>& types, const std::vector<std::vector<uint8_t>>& cells);
// Split a composite key back into its cells. Throws on malformed input.
std::vector<std::vector<uint8_t>> decode_key(const std::vector<ColType>& types, const std::string& key);
