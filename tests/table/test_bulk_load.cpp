// Table::bulk_insert's load order (#69): rows are loaded by key even when
// keys agree on their first 8 bytes or differ only by trailing zero bytes,
// and of rows with the same key the first one given wins.

#include "catalog/table.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <map>
#include <random>
#include <string>
#include <vector>

static const char* TRIE_FILE = "/tmp/test_bulk_load.trie";
static const char* HEAP_FILE = "/tmp/test_bulk_load.heap";
static const std::string BLOOM_FILE = std::string(TRIE_FILE) + ".bloom";
static void cleanup() { std::remove(TRIE_FILE); std::remove(HEAP_FILE); std::remove(BLOOM_FILE.c_str()); }

static std::vector<uint8_t> bytes(const std::string& s) { return {s.begin(), s.end()}; }

int main() {
    cleanup();
    Schema s{"t", {{"id", ColType::VARCHAR, 24}, {"v", ColType::VARCHAR, 8}}, 0};
    std::mt19937_64 rng(3);

    // Keys sharing a 10-byte prefix, keys ending in zero bytes ("ab", "ab\0",
    // "ab\0\0" look alike in their first 8 bytes), and repeats.
    std::vector<std::string> keys;
    for (int i = 0; i < 3000; i++) keys.push_back("shared/pre" + std::to_string(rng() % 2000));
    for (int i = 0; i < 300; i++) keys.push_back(std::string("ab") + std::string(rng() % 4, '\0') + char('a' + rng() % 3));
    for (int z = 0; z < 4; z++) keys.push_back(std::string("ab") + std::string(z, '\0'));
    std::shuffle(keys.begin(), keys.end(), rng);

    std::vector<Row> rows;
    std::map<std::string, std::string> want;   // first value given per key
    for (size_t i = 0; i < keys.size(); i++) {
        std::string v = std::to_string(i % 100000);
        rows.push_back({bytes(keys[i]), bytes(v)});
        want.emplace(keys[i], v);
    }

    Table t = Table::create(s, TRIE_FILE, HEAP_FILE);
    assert(t.bulk_insert(std::move(rows)) == want.size());

    // A scan over everything returns the keys in order, each with its first value.
    std::vector<Row> got = t.range(bytes(""), bytes(std::string(24, '\xff')));
    assert(got.size() == want.size());
    auto it = want.begin();
    for (const Row& r : got) {
        assert(std::string(r[0].begin(), r[0].end()) == it->first);
        assert(std::string(r[1].begin(), r[1].end()) == it->second);
        ++it;
    }
    for (const auto& [k, v] : want) {
        Row r;
        assert(t.lookup(bytes(k), &r) && std::string(r[1].begin(), r[1].end()) == v);
    }

    cleanup();
    std::puts("test_bulk_load: all passed");
    return 0;
}
