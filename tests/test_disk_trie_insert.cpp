#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include <algorithm>
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

// Single inserts leave dirty chains in the hot cache; a later bulk_insert must
// flush them before resetting the cache, or those keys are lost.
static void test_bulk_insert_after_single_inserts() {
    cleanup();
    std::mt19937_64 rng(3);
    auto make_key = [&] { uint64_t v = rng(); return std::string(reinterpret_cast<const char*>(&v), 8); };
    std::vector<std::string> single, bulk;
    for (int i = 0; i < 300; i++)  single.push_back(make_key());
    for (int i = 0; i < 3000; i++) bulk.push_back(make_key());
    std::sort(bulk.begin(), bulk.end());
    {
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        DiskTrie t(dm);
        for (size_t i = 0; i < single.size(); i++) t.insert(single[i], RecordPtr{i, 0});
        std::vector<std::pair<std::string, RecordPtr>> kvs;
        for (size_t i = 0; i < bulk.size(); i++) kvs.push_back({bulk[i], RecordPtr{10000 + i, 0}});
        t.bulk_insert(std::move(kvs));
        for (size_t i = 0; i < single.size(); i++) assert(t.lookup(single[i]));
    }
    // And on disk: a fresh trie with an empty cache reads everything back.
    auto dm_ptr = DiskManager::open(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    for (size_t i = 0; i < single.size(); i++) {
        RecordPtr p;
        assert(t.lookup(single[i], &p) && p.block_id == i);
    }
    for (size_t i = 0; i < bulk.size(); i++) {
        RecordPtr p;
        assert(t.lookup(bulk[i], &p) && p.block_id == 10000 + i);
    }
}

// Removes also leave dirty chains in the hot cache; a later bulk_insert must
// not discard them, or removed keys come back.
static void test_bulk_insert_after_removes() {
    cleanup();
    std::mt19937_64 rng(5);
    auto make_key = [&] { uint64_t v = rng(); return std::string(reinterpret_cast<const char*>(&v), 8); };
    std::vector<std::string> keys, bulk;
    for (int i = 0; i < 2000; i++) keys.push_back(make_key());
    for (int i = 0; i < 2000; i++) bulk.push_back(make_key());
    std::sort(bulk.begin(), bulk.end());
    {
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        DiskTrie t(dm);
        for (size_t i = 0; i < keys.size(); i++) t.insert(keys[i], RecordPtr{i, 0});
    }
    auto dm_ptr = DiskManager::open(TEST_FILE); auto& dm = *dm_ptr;
    {
        DiskTrie t(dm);
        for (size_t i = 0; i < keys.size(); i += 2) assert(t.remove(keys[i]));
        std::vector<std::pair<std::string, RecordPtr>> kvs;
        for (size_t i = 0; i < bulk.size(); i++) kvs.push_back({bulk[i], RecordPtr{10000 + i, 0}});
        t.bulk_insert(std::move(kvs));
        for (size_t i = 0; i < keys.size(); i++) assert(t.lookup(keys[i]) == (i % 2 == 1));
    }
    DiskTrie t(dm);
    for (size_t i = 0; i < keys.size(); i++) assert(t.lookup(keys[i]) == (i % 2 == 1));
    for (const auto& k : bulk) assert(t.lookup(k));
}

// An opened trie defers its per-chain counts to the first write; the lazily
// rebuilt counts must still drive rebalancing correctly.
static void test_counts_rebuilt_lazily_on_first_write() {
    cleanup();
    std::mt19937_64 rng(9);
    auto rand_key = [&] { uint64_t v = rng(); return std::string(reinterpret_cast<const char*>(&v), 8); };
    std::vector<std::string> keys;
    {
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        DiskTrie t(dm);
        for (int i = 0; i < 512; i++) { keys.push_back(rand_key()); t.insert(keys.back(), RecordPtr{1, 0}); }
    }
    auto dm_ptr = DiskManager::open(TEST_FILE); auto& dm = *dm_ptr;
    {
        DiskTrie t(dm);
        assert(!t.counts_ready());               // open did not walk the trie
        for (const auto& k : keys) assert(t.lookup(k));
        assert(!t.counts_ready());               // reads never need counts

        // Adversarial writes after reopen: 2000 keys sharing a 6-byte prefix pile
        // onto one subtree and force flips, plus removes of half the old keys.
        for (int i = 0; i < 2000; i++) {
            std::string k = "PREFIX" + std::string(1, char(i >> 8)) + std::string(1, char(i & 0xFF));
            t.insert(k, RecordPtr{2, 0});
            keys.push_back(k);
        }
        assert(t.counts_ready());
        for (size_t i = 0; i < 512; i += 2) assert(t.remove(keys[i]));
    }
    DiskTrie t(dm);
    size_t max_chains = 0, n = 0;
    for (size_t i = 0; i < keys.size(); i++) {
        bool removed = i < 512 && i % 2 == 0;
        size_t chains = 0;
        assert(t.lookup(keys[i], nullptr, &chains) == !removed);
        if (!removed) { max_chains = std::max(max_chains, chains); n++; }
    }
    double log2n = 0;
    for (size_t x = n; x > 1; x >>= 1) log2n++;
    assert(max_chains <= static_cast<size_t>(2.0 * log2n));
}

// Keys far longer than 32 bytes (the old 8-bit split limit, #32), sharing a
// 100-byte prefix: chains with long paths and many nodes.
static void test_long_keys() {
    cleanup();
    std::mt19937_64 rng(21);
    std::string common(100, 'p');
    std::vector<std::string> keys;
    for (int i = 0; i < 2000; i++) {
        std::string k = (i % 3 ? common : std::string()) + std::to_string(i) + "/";
        size_t extra = 40 + rng() % 200;
        while (k.size() < extra) k.push_back(static_cast<char>('a' + rng() % 4));
        keys.push_back(k);
    }
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    {
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        DiskTrie t(dm);
        std::vector<size_t> order(keys.size());
        for (size_t i = 0; i < order.size(); i++) order[i] = i;
        std::shuffle(order.begin(), order.end(), rng);
        for (size_t i : order) t.insert(keys[i], RecordPtr{i, 0});
        for (size_t i = 0; i < keys.size(); i += 5) assert(t.remove(keys[i]));
        t.compact();
    }
    auto dm_ptr = DiskManager::open(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    std::vector<std::pair<std::string, RecordPtr>> all;
    t.range_scan(std::string(), std::string(400, '\xFF'), all);
    size_t j = 0;
    for (size_t i = 0; i < keys.size(); i++) {
        RecordPtr p;
        bool removed = i % 5 == 0;
        assert(t.lookup(keys[i], &p) == !removed);
        if (removed) continue;
        assert(p.block_id == i);
        assert(all[j].first == keys[i]);
        j++;
    }
    assert(j == all.size());
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
    test_bulk_insert_after_single_inserts();
    test_bulk_insert_after_removes();
    test_counts_rebuilt_lazily_on_first_write();
    test_long_keys();
    return 0;
}
