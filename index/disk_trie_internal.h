#pragma once
// Helpers shared by the DiskTrie implementation files (disk_trie*.cpp); not
// part of the public interface.
#include "index/chain.h"
#include <algorithm>
#include <string>
#include <vector>

inline size_t key_nibbles(const std::string& key) { return key.size() * 2; }

// Key nibbles from nibble `from` to the end.
inline std::vector<uint8_t> key_suffix(const std::string& key, size_t from) {
    std::vector<uint8_t> s(key_nibbles(key) - from);
    for (size_t i = 0; i < s.size(); i++) s[i] = key_nibble(key, from + i);
    return s;
}

// A one-key bucket for the subtree starting at key nibble pos.
inline ChainData single_bucket(const std::string& key, size_t pos, RecordPtr record) {
    return make_bucket({{key_suffix(key, pos), record}}, static_cast<uint8_t>(pos % 2));
}

inline bool suffix_less(const BucketEntry& a, const BucketEntry& b) { return a.suffix < b.suffix; }

// Insert a light child keeping lights sorted by digit; returns its index.
inline size_t insert_light(ChainNode& node, LightChild l) {
    auto it = std::lower_bound(node.lights.begin(), node.lights.end(), l.digit,
                               [](const LightChild& a, uint8_t d) { return a.digit < d; });
    size_t at = static_cast<size_t>(it - node.lights.begin());
    node.lights.insert(it, l);
    return at;
}
