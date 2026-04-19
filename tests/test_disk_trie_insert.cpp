#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include <cassert>
#include <cstdio>
#include <string>
#include <vector>
#include <random>

static const char* TEST_FILE = "/tmp/test_disk_trie_insert.db";
static void cleanup() { std::remove(TEST_FILE); }

static void test_insert_single() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    t.insert("ab", RecordPtr{1,0});
    assert( t.lookup("ab"));
    assert(!t.lookup("ac"));
    assert(!t.lookup("a"));
    assert(!t.lookup("abc"));
}

static void test_insert_two_diverging() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    t.insert("ab", RecordPtr{1,0});
    t.insert("cd", RecordPtr{1,0});
    assert(t.lookup("ab"));
    assert(t.lookup("cd"));
    assert(!t.lookup("ac"));
}

static void test_insert_prefix_then_extension() {
    // "ab" then "abc" — extension case
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    t.insert("ab", RecordPtr{1,0});
    t.insert("abc", RecordPtr{1,0});
    assert(t.lookup("ab"));
    assert(t.lookup("abc"));
    assert(!t.lookup("a"));
    assert(!t.lookup("abcd"));
}

static void test_insert_extension_then_prefix() {
    // "abc" then "ab" — key-ends-mid-chain case
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    t.insert("abc", RecordPtr{1,0});
    t.insert("ab", RecordPtr{1,0});
    assert(t.lookup("abc"));
    assert(t.lookup("ab"));
    assert(!t.lookup("a"));
}

static void test_insert_duplicate() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    t.insert("ab", RecordPtr{1,0});
    t.insert("ab", RecordPtr{1,0}); // should be a no-op
    assert(t.lookup("ab"));
}

static void test_insert_many_and_lookup_all() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);

    std::mt19937 rng(99);
    std::uniform_int_distribution<int> d(0, 255);

    std::vector<std::string> keys;
    for (int i = 0; i < 256; i++) {
        std::string k(4, '\0'); // 32-bit keys
        for (char& c : k) c = static_cast<char>(d(rng));
        keys.push_back(k);
        t.insert(k, RecordPtr{1,0});
    }

    for (const auto& k : keys)
        assert(t.lookup(k));
}

static void test_chains_logarithmic_after_insert() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);

    const int n = 512;
    std::mt19937 rng(7);
    std::uniform_int_distribution<int> d(0, 255);

    std::vector<std::string> keys;
    for (int i = 0; i < n; i++) {
        std::string k(8, '\0'); // 64-bit keys
        for (char& c : k) c = static_cast<char>(d(rng));
        keys.push_back(k);
        t.insert(k, RecordPtr{1,0});
    }

    size_t max_chains = 0;
    for (const auto& k : keys) {
        size_t chains = 0;
        assert(t.lookup(k, nullptr, &chains));
        if (chains > max_chains) max_chains = chains;
    }

    // O(log n) bound with generous constant
    double log2n = 0;
    for (int x = n; x > 1; x >>= 1) log2n++;
    assert(max_chains <= static_cast<size_t>(2.0 * log2n));
}

// Adversarial: 1 key on the "0" side, N on the "1" side.
// Without rebalancing every "1"-side key starts with a light edge (≥ 2 chains).
// With rebalancing the flip fires at bit 0 and the N-key side becomes heavy,
// so their first crossing is eliminated. Max chains should be O(log N).
static void test_flips_per_insert_bounded() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);

    const int n = 512;
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> d(0, 255);

    size_t max_flips = 0;
    double log2n    = 0; for (int x = n; x > 1; x >>= 1) log2n++;

    for (int i = 0; i < n; i++) {
        std::string k(8, '\0');
        for (char& c : k) c = static_cast<char>(d(rng));
        size_t flips = t.insert(k, RecordPtr{1,0});
        if (flips > max_flips) max_flips = flips;
    }

    // Each insert can flip at most once per chain crossing = O(log n).
    assert(max_flips <= static_cast<size_t>(log2n + 1));
}

static void test_rebalancing_adversarial() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);

    const int N = 100;

    // 1 key: all zeros (bit 0 = 0).
    t.insert(std::string(4, '\x00'), RecordPtr{1,0});

    // N keys: bit 0 = 1, unique across bytes 1..3.
    auto make_key = [](int i) {
        std::string k(4, '\0');
        k[0] = '\x80'; // bit 0 = 1, bits 1..7 = 0
        k[1] = static_cast<char>((i >> 16) & 0xFF);
        k[2] = static_cast<char>((i >> 8)  & 0xFF);
        k[3] = static_cast<char>( i        & 0xFF);
        return k;
    };
    for (int i = 0; i < N; i++)
        t.insert(make_key(i), RecordPtr{1,0});

    // All keys must be found.
    assert(t.lookup(std::string(4, '\x00')));
    for (int i = 0; i < N; i++)
        assert(t.lookup(make_key(i)));

    // With rebalancing: N-key side is heavy after the flip at bit 0.
    // Max chains is 1 (root chain) + internal depth ≤ O(log N).
    // Without rebalancing it would be ≥ 2 for every single key.
    size_t max_chains = 0;
    for (int i = 0; i < N; i++) {
        size_t chains = 0;
        t.lookup(make_key(i), nullptr, &chains);
        if (chains > max_chains) max_chains = chains;
    }
    // All N keys share first byte 0x80 so they differ only in bytes 1..3 (24 bits).
    // O(log N) ≈ 7. Generous constant to account for imperfect log2 rebalancing.
    double log2n = 0; for (int x = N; x > 1; x >>= 1) log2n++;
    assert(max_chains <= static_cast<size_t>(2.0 * log2n + 2));
}

int main() {
    test_insert_single();
    test_insert_two_diverging();
    test_insert_prefix_then_extension();
    test_insert_extension_then_prefix();
    test_insert_duplicate();
    test_insert_many_and_lookup_all();
    test_chains_logarithmic_after_insert();
    test_flips_per_insert_bounded();
    test_rebalancing_adversarial();
    cleanup();
    return 0;
}
