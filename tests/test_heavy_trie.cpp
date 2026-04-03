#include "heavy_trie.h"
#include <cassert>
#include <cmath>
#include <string>
#include <vector>
#include <random>
#include <algorithm>

static void test_insert_query_basic() {
    HeavyTrie t;
    t.insert("ab");
    assert(t.query("ab"));
    assert(!t.query("ac"));
    assert(!t.query("a"));
    assert(!t.query("abc"));
}

static void test_missing_key() {
    HeavyTrie t;
    t.insert("hello");
    assert(!t.query("world"));
    assert(!t.query("hell"));
    assert(!t.query("helloo"));
}

static void test_chains_single_key() {
    HeavyTrie t;
    t.insert("a");
    size_t chains = 0;
    assert(t.query("a", &chains));
    // Only one key: every edge is heavy (no siblings), so only 1 chain.
    assert(chains == 1);
}

static void test_chains_logarithmic() {
    // Insert n random keys, verify max chain crossings <= C * log2(n) for all keys.
    const int n = 1024;
    const double C = 2.0; // generous constant

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> byte_dist(0, 255);

    auto rand_key = [&]() {
        std::string k(8, '\0'); // 64-bit keys
        for (char& c : k) c = static_cast<char>(byte_dist(rng));
        return k;
    };

    HeavyTrie t;
    std::vector<std::string> keys;
    keys.reserve(n);
    for (int i = 0; i < n; i++) {
        keys.push_back(rand_key());
        t.insert(keys.back());
    }

    double log2n = std::log2(static_cast<double>(n));
    size_t max_chains = 0;

    for (const auto& key : keys) {
        size_t chains = 0;
        assert(t.query(key, &chains));
        max_chains = std::max(max_chains, chains);
    }

    assert(static_cast<double>(max_chains) <= C * log2n);
}

int main() {
    test_insert_query_basic();
    test_missing_key();
    test_chains_single_key();
    test_chains_logarithmic();
    return 0;
}
