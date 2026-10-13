#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include <cassert>
#include <cstdio>
#include <vector>
#include <random>
#include <algorithm>

static const char* TEST_FILE = "/tmp/test_disk_trie_remove.db";
static void cleanup() { std::remove(TEST_FILE); }

static void test_remove_basic() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);

    t.insert("ab", RecordPtr{1,0});
    assert( t.lookup("ab"));
    assert( t.remove("ab"));
    assert(!t.lookup("ab"));
    // Second remove returns false.
    assert(!t.remove("ab"));
}

static void test_remove_missing() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    t.insert("ab", RecordPtr{1,0});
    assert(!t.remove("cd"));
    assert( t.lookup("ab")); // unaffected
}

static void test_remove_prefix_key() {
    // "ab" and "abc" both inserted; remove "ab", "abc" still there.
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    t.insert("ab", RecordPtr{1,0});
    t.insert("abc", RecordPtr{1,0});
    assert(t.remove("ab"));
    assert(!t.lookup("ab"));
    assert( t.lookup("abc"));
}

static void test_remove_extension_key() {
    // "ab" and "abc"; remove "abc", "ab" still there.
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    t.insert("ab", RecordPtr{1,0});
    t.insert("abc", RecordPtr{1,0});
    assert(t.remove("abc"));
    assert( t.lookup("ab"));
    assert(!t.lookup("abc"));
}

static void test_remove_all_then_reinsert() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    t.insert("foo", RecordPtr{1,0});
    t.insert("bar", RecordPtr{1,0});
    assert(t.remove("foo"));
    assert(t.remove("bar"));
    assert(!t.lookup("foo"));
    assert(!t.lookup("bar"));
    t.insert("foo", RecordPtr{1,0});
    assert( t.lookup("foo"));
    assert(!t.lookup("bar"));
}

static void test_remove_many() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);

    std::mt19937 rng(77);
    std::uniform_int_distribution<int> d(0, 255);

    std::vector<std::string> keys;
    for (int i = 0; i < 128; i++) {
        std::string k(4, '\0');
        for (char& c : k) c = static_cast<char>(d(rng));
        keys.push_back(k);
        t.insert(k, RecordPtr{1,0});
    }

    // Remove first half.
    for (int i = 0; i < 64; i++) {
        assert(t.remove(keys[i]));
        assert(!t.lookup(keys[i]));
    }
    // Second half still present.
    for (int i = 64; i < 128; i++)
        assert(t.lookup(keys[i]));
}

static void test_remove_triggers_reverse_flip() {
    // Insert 1 key on "0" side, many on "1" side (rebalancing fires, "1" becomes heavy).
    // Then delete all "1"-side keys — reverse flip should fire, "0" side becomes heavy again.
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);

    auto make_one  = []() { return std::string(4, '\x00'); };
    auto make_many = [](int i) {
        std::string k(4, '\0');
        k[0] = '\x80';
        k[1] = static_cast<char>((i >> 16) & 0xFF);
        k[2] = static_cast<char>((i >> 8)  & 0xFF);
        k[3] = static_cast<char>( i        & 0xFF);
        return k;
    };

    t.insert(make_one(), RecordPtr{1,0});
    for (int i = 0; i < 10; i++) t.insert(make_many(i), RecordPtr{1,0});

    // Remove the many-side keys.
    for (int i = 0; i < 10; i++) assert(t.remove(make_many(i)));

    // One key still present.
    assert( t.lookup(make_one()));
    for (int i = 0; i < 10; i++) assert(!t.lookup(make_many(i)));
}

// #38: removes leave no dead structure. Tiny buckets and no merging keep the
// chains, so the pruning itself is what is checked: removing the key held by
// a record-only node drops the node, and emptying a light subtree unlinks it.
static bool has_dead_structure(DiskManager& dm, uint64_t addr) {
    ChainData c = dm.read_chain_at(addr);
    if (c.bucket) return bucket_size(c) == 0;
    for (const auto& n : c.nodes) {
        if (!n.record.valid() && n.lights.empty()) return true;
        for (const auto& l : n.lights) if (has_dead_structure(dm, l.block)) return true;
    }
    return false;
}

static void test_remove_prunes_empty_structure() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm, "", TrieOptions{40, 0});
    const std::vector<std::string> keys = {"abcdef1", "abcdef2", "abcdef3", "ab", "abX", "abXY", "abZ"};
    for (size_t i = 0; i < 4; i++) t.insert(keys[i], RecordPtr{i + 1, 0});
    assert(t.remove("ab"));       // a record-only node: removing its key drops it
    t.flush();
    assert(!has_dead_structure(dm, dm.root_block()));
    for (size_t i = 3; i < keys.size(); i++) t.insert(keys[i], RecordPtr{i + 1, 0});

    assert(t.remove("ab"));       // its node keeps the "X" and "Z" children
    assert(t.remove("abZ"));      // the "Z" subtree empties: unlinked
    assert(t.remove("abX"));
    assert(t.remove("abXY"));     // "X" empties too, and with it the node
    t.flush();
    assert(!has_dead_structure(dm, dm.root_block()));
    assert(t.pruned() >= 2);
    for (const char* k : {"abcdef1", "abcdef2", "abcdef3"}) assert(t.lookup(k));
    for (const char* k : {"ab", "abX", "abXY", "abZ"}) assert(!t.lookup(k));

    for (const char* k : {"ab", "abQ"}) t.insert(k, RecordPtr{99, 0});   // and it still takes inserts
    assert(t.lookup("ab") && t.lookup("abQ"));
}

int main() {
    test_remove_basic();
    test_remove_missing();
    test_remove_prefix_key();
    test_remove_extension_key();
    test_remove_all_then_reinsert();
    test_remove_many();
    test_remove_triggers_reverse_flip();
    cleanup();
    test_remove_prunes_empty_structure();
    return 0;
}
