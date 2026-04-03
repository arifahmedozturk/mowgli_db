#pragma once
#include "node.h"
#include <string>
#include <cstdint>

class HeavyTrie {
public:
    HeavyTrie() = default;
    ~HeavyTrie() { delete root_; }

    void insert(const std::string& key);
    bool query(const std::string& key, size_t* chains_out = nullptr) const;

private:
    Node* root_ = nullptr;

    static bool bit_at(const std::string& key, size_t i) {
        return (static_cast<uint8_t>(key[i / 8]) >> (7 - (i % 8))) & 1;
    }

    static size_t bit_len(const std::string& key) {
        return key.size() * 8;
    }
};
