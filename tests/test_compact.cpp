#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <string>
#include <vector>

static const char* TEST_FILE = "/tmp/test_compact.db";
static void cleanup() { std::remove(TEST_FILE); }

static RecordPtr make_rec(uint64_t id) { return {id, 0}; }

// Insert keys, apply a compact function, verify all lookups and a full range scan.
static void run_compact_test(void (DiskTrie::*compact_fn)()) {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);

    // Keys chosen to exercise both heavy-path-goes-0 and heavy-path-goes-1
    // nodes so both branches of compact_assign_lex are exercised.
    std::vector<std::string> keys = {
        std::string(1, '\x10'),   // 00010000
        std::string(1, '\x20'),   // 00100000
        std::string(1, '\x40'),   // 01000000
        std::string(1, '\x60'),   // 01100000
        std::string(1, '\x80'),   // 10000000
        std::string(1, '\xa0'),   // 10100000
        std::string(1, '\xc0'),   // 11000000
        std::string(1, '\xe0'),   // 11100000
    };

    for (size_t i = 0; i < keys.size(); i++)
        t.insert(keys[i], make_rec(100 + i));

    (t.*compact_fn)();

    // All lookups must succeed after compaction.
    for (size_t i = 0; i < keys.size(); i++) {
        RecordPtr ptr;
        assert(t.lookup(keys[i], &ptr));
        assert(ptr.block_id == 100 + i);
    }

    // Full range scan must return all keys in sorted order.
    std::vector<std::pair<std::string, RecordPtr>> out;
    t.range_scan(keys.front(), keys.back(), out);
    assert(out.size() == keys.size());

    std::vector<std::string> sorted = keys;
    std::sort(sorted.begin(), sorted.end());
    for (size_t i = 0; i < sorted.size(); i++)
        assert(out[i].first == sorted[i]);
}

// Longer keys (multi-byte) to stress non-zero bit_phase chains in lex layout.
static void run_compact_lex_multibyte() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);

    std::vector<std::string> keys;
    for (int i = 0; i < 64; i++) {
        char buf[2] = { static_cast<char>(i * 2), static_cast<char>(i) };
        keys.push_back(std::string(buf, 2));
    }

    for (size_t i = 0; i < keys.size(); i++)
        t.insert(keys[i], make_rec(200 + i));

    t.compact_lex();

    for (size_t i = 0; i < keys.size(); i++) {
        RecordPtr ptr;
        assert(t.lookup(keys[i], &ptr));
        assert(ptr.block_id == 200 + i);
    }

    // Range scan over a sub-range.
    std::string lo = keys[10], hi = keys[20];
    std::vector<std::pair<std::string, RecordPtr>> out;
    t.range_scan(lo, hi, out);

    std::vector<std::string> expected;
    for (auto& k : keys)
        if (k >= lo && k <= hi) expected.push_back(k);
    std::sort(expected.begin(), expected.end());

    assert(out.size() == expected.size());
    for (size_t i = 0; i < expected.size(); i++)
        assert(out[i].first == expected[i]);
}

// compact() and compact_lex() on the same trie must produce identical lookup results.
static void test_compact_vs_compact_lex_agree() {
    std::vector<std::string> keys;
    for (int i = 0; i < 32; i++) {
        char buf[2] = { static_cast<char>(i * 3 + 1), static_cast<char>(i) };
        keys.push_back(std::string(buf, 2));
    }

    // Run compact() variant.
    std::vector<RecordPtr> results_compact(keys.size());
    {
        cleanup();
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        DiskTrie t(dm);
        for (size_t i = 0; i < keys.size(); i++)
            t.insert(keys[i], make_rec(300 + i));
        t.compact();
        for (size_t i = 0; i < keys.size(); i++) {
            RecordPtr ptr;
            assert(t.lookup(keys[i], &ptr));
            results_compact[i] = ptr;
        }
    }

    // Run compact_lex() variant.
    std::vector<RecordPtr> results_lex(keys.size());
    {
        cleanup();
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        DiskTrie t(dm);
        for (size_t i = 0; i < keys.size(); i++)
            t.insert(keys[i], make_rec(300 + i));
        t.compact_lex();
        for (size_t i = 0; i < keys.size(); i++) {
            RecordPtr ptr;
            assert(t.lookup(keys[i], &ptr));
            results_lex[i] = ptr;
        }
    }

    // Both must return identical record pointers for every key.
    for (size_t i = 0; i < keys.size(); i++) {
        assert(results_compact[i].block_id == results_lex[i].block_id);
        assert(results_compact[i].slot     == results_lex[i].slot);
    }
}

int main() {
    run_compact_test(&DiskTrie::compact);
    run_compact_test(&DiskTrie::compact_lex);
    run_compact_lex_multibyte();
    test_compact_vs_compact_lex_agree();
    cleanup();
    return 0;
}
