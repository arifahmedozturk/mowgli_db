#include "catalog/table.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>

static const char* TRIE_FILE = "/tmp/test_table.trie";
static const char* HEAP_FILE = "/tmp/test_table.heap";
static void cleanup() { std::remove(TRIE_FILE); std::remove(HEAP_FILE); }

// Helper: build a Schema with (id UINT64 PK, name VARCHAR(32))
static Schema make_schema() {
    return Schema{
        "users",
        {
            {"id",   ColType::UINT64,  8},
            {"name", ColType::VARCHAR, 32},
        },
        0 // pk_col = id
    };
}

// Helper: encode a uint64 as 8 little-endian bytes.
static std::vector<uint8_t> u64(uint64_t v) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; i++) b[i] = (v >> (i * 8)) & 0xFF;
    return b;
}

static std::vector<uint8_t> str(const char* s) {
    return std::vector<uint8_t>(s, s + strlen(s));
}

static void test_insert_and_lookup() {
    cleanup();
    auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);

    Row row = {u64(1), str("alice")};
    assert(t.insert(row));

    Row out;
    assert(t.lookup(u64(1), &out));
    assert(out[0] == u64(1));
    assert(out[1] == str("alice"));
}

static void test_duplicate_insert() {
    cleanup();
    auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);

    assert( t.insert({u64(1), str("alice")}));
    assert(!t.insert({u64(1), str("alice2")})); // same PK
}

static void test_missing_lookup() {
    cleanup();
    auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);
    assert(!t.lookup(u64(99)));
}

static void test_remove() {
    cleanup();
    auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);

    assert(t.insert({u64(1), str("alice")}));
    assert(t.remove(u64(1)));
    assert(!t.lookup(u64(1)));
    assert(!t.remove(u64(1))); // already gone
}

static void test_multiple_rows() {
    cleanup();
    auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);

    for (uint64_t i = 0; i < 50; i++)
        assert(t.insert({u64(i), str(("user" + std::to_string(i)).c_str())}));

    for (uint64_t i = 0; i < 50; i++) {
        Row out;
        assert(t.lookup(u64(i), &out));
        assert(out[0] == u64(i));
    }

    assert(!t.lookup(u64(99)));
}

static void test_remove_and_reinsert() {
    cleanup();
    auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);

    assert(t.insert({u64(7), str("bob")}));
    assert(t.remove(u64(7)));
    assert(t.insert({u64(7), str("carol")}));

    Row out;
    assert(t.lookup(u64(7), &out));
    assert(out[1] == str("carol"));
}

int main() {
    test_insert_and_lookup();
    test_duplicate_insert();
    test_missing_lookup();
    test_remove();
    test_multiple_rows();
    test_remove_and_reinsert();
    cleanup();
    return 0;
}
