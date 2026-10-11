#pragma once
// Benchmark key distributions, shared by bench (heavy-trie) and bench_btree (B-tree baselines)
// so both run on identical keys.

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

// Big-endian, so bytewise key order matches numeric order.
inline std::vector<uint8_t> encode_u64(uint64_t v) {
    std::vector<uint8_t> b(8);
    for (int i = 7; i >= 0; i--) { b[i] = v & 0xFF; v >>= 8; }
    return b;
}

// ---- key distributions ----

// Primary key exactly as stored (raw bytes; the trie orders keys bytewise).
using Key = std::vector<uint8_t>;

inline constexpr const char* KEY_DISTS[] = {"random_u64", "seq_u64", "prefix_str", "varlen_str"};

// String keys stay <= 32 bytes: longer keys hit task #32 (uint8_t split_bit).
inline constexpr uint16_t STR_KEY_MAX = 32;

inline bool is_u64_dist(const std::string& dist) { return dist == "random_u64" || dist == "seq_u64"; }

// N keys in insertion order. random_u64: uniform u64; seq_u64: 0..N-1 in order;
// prefix_str: "tenantNN/user/<10-digit id>" (16 tenants, 24 B, random order);
// varlen_str: distinct random alphanumerics of 4..32 bytes.
inline std::vector<Key> make_keys(const std::string& dist, size_t n, std::mt19937_64& rng) {
    std::vector<Key> keys;
    keys.reserve(n);
    if (dist == "random_u64") {
        std::uniform_int_distribution<uint64_t> d;
        while (keys.size() < n) keys.push_back(encode_u64(d(rng)));
    } else if (dist == "seq_u64") {
        for (uint64_t i = 0; i < n; i++) keys.push_back(encode_u64(i));
    } else if (dist == "prefix_str") {
        char buf[48];
        for (uint64_t i = 0; i < n; i++) {
            snprintf(buf, sizeof(buf), "tenant%02llu/user/%010llu",
                     (unsigned long long)(i % 16), (unsigned long long)i);
            keys.emplace_back(buf, buf + strlen(buf));
        }
        std::shuffle(keys.begin(), keys.end(), rng);
    } else if (dist == "varlen_str") {
        static constexpr char ALPHA[] = "0123456789abcdefghijklmnopqrstuvwxyz";
        std::uniform_int_distribution<int> len(4, STR_KEY_MAX), ch(0, 35);
        std::unordered_set<std::string> seen;
        while (keys.size() < n) {
            std::string k(len(rng), ' ');
            for (char& c : k) c = ALPHA[ch(rng)];
            if (seen.insert(k).second) keys.emplace_back(k.begin(), k.end());
        }
    } else {
        throw std::runtime_error("unknown --keys distribution: " + dist);
    }
    return keys;
}
