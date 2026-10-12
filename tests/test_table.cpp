#include "catalog/table.h"
#include <cassert>
#include <random>
#include <map>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>

static const char* TRIE_FILE = "/tmp/test_table.trie";
static const char* HEAP_FILE = "/tmp/test_table.heap";
static const std::string BLOOM_FILE = std::string(TRIE_FILE) + ".bloom";
static void cleanup() { std::remove(TRIE_FILE); std::remove(HEAP_FILE); std::remove(BLOOM_FILE.c_str()); }
static bool exists(const std::string& p) { return std::filesystem::exists(p); }

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

static void insert_range(Table& t, uint64_t lo, uint64_t hi) {
    for (uint64_t i = lo; i < hi; i++) assert(t.insert({u64(i), str("x")}));
}

static void assert_all_found(Table& t, uint64_t n) {
    for (uint64_t i = 0; i < n; i++) assert(t.lookup(u64(i)));
    assert(!t.lookup(u64(n + 12345)));
}

// The Bloom filter is saved on clean close and loaded (not rebuilt) on reopen.
static void test_bloom_persisted_across_reopen() {
    cleanup();
    {
        auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);
        insert_range(t, 0, 500);
    }
    assert(exists(BLOOM_FILE));
    auto t = Table::open(make_schema(), TRIE_FILE, HEAP_FILE);
    assert(t.bloom_loaded());
    // Deleted while open, so a crash before the next clean close can't leave it stale.
    assert(!exists(BLOOM_FILE));
    insert_range(t, 500, 600);
    assert_all_found(t, 600);
}

// A sidecar from before later writes must be rejected and the filter rebuilt;
// otherwise the newer keys would be reported missing.
static void test_stale_bloom_rejected() {
    cleanup();
    std::string stale = BLOOM_FILE + ".old";
    {
        auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);
        insert_range(t, 0, 300);
    }
    std::filesystem::copy_file(BLOOM_FILE, stale, std::filesystem::copy_options::overwrite_existing);
    {
        auto t = Table::open(make_schema(), TRIE_FILE, HEAP_FILE);
        insert_range(t, 300, 600);
    }
    std::filesystem::rename(stale, BLOOM_FILE);  // like a crash leaving an old filter
    auto t = Table::open(make_schema(), TRIE_FILE, HEAP_FILE);
    assert(!t.bloom_loaded());
    assert_all_found(t, 600);
}

// No sidecar (e.g. after a crash): rebuild from the stored keys.
static void test_missing_bloom_rebuilt() {
    cleanup();
    {
        auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);
        insert_range(t, 0, 400);
    }
    std::remove(BLOOM_FILE.c_str());
    auto t = Table::open(make_schema(), TRIE_FILE, HEAP_FILE);
    assert(!t.bloom_loaded());
    insert_range(t, 400, 450);
    assert_all_found(t, 450);
}

// #65: random single inserts (with duplicates, deletes and updates mixed in)
// keep every row intact while heap pages are split, keep rows of neighbouring
// keys on the same page, and keep the heap about as compact as a bulk load.
// Big-endian, as the engine encodes numbers: byte order = numeric order.
static std::vector<uint8_t> be64(uint64_t v) {
    std::vector<uint8_t> b(8);
    for (int i = 7; i >= 0; i--) { b[i] = v & 0xFF; v >>= 8; }
    return b;
}

static void test_rows_clustered_by_key() {
    cleanup();
    auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);
    std::mt19937_64 rng(3);
    std::map<uint64_t, std::string> model;
    auto name_for = [](uint64_t k, int v) { return "n" + std::to_string(k % 1000) + "v" + std::to_string(v); };
    for (int i = 0; i < 20000; i++) {
        uint64_t k = rng() % 1'000'000;
        std::string name = name_for(k, 0);
        bool fresh = !model.count(k);
        assert(t.insert({be64(k), str(name.c_str())}) == fresh);
        if (fresh) model[k] = name;
        if (i % 7 == 0 && !model.empty()) {           // delete an existing key
            auto it = model.begin();
            std::advance(it, static_cast<long>(rng() % model.size()));
            assert(t.remove(be64(it->first)));
            model.erase(it);
        }
        if (i % 11 == 0 && !model.empty()) {          // update one (same row size)
            auto it = model.begin();
            std::advance(it, static_cast<long>(rng() % model.size()));
            it->second = name_for(it->first, 1);
            assert(t.update({be64(it->first), str(it->second.c_str())}));
        }
    }
    assert(t.record_count() == model.size());

    // Every row intact, and count heap page changes walking keys in order.
    size_t switches = 0;
    uint64_t prev_page = NULL_BLOCK;
    for (const auto& [k, name] : model) {
        Row out;
        assert(t.lookup(be64(k), &out) && out[1] == str(name.c_str()));
        RecordPtr p;
        assert(t.locate(be64(k), &p));
        switches += prev_page != NULL_BLOCK && p.block_id != prev_page;
        prev_page = p.block_id;
    }
    // A row here is 42 B + a 4 B slot, so ~177 fit a page: `packed` is the page
    // count if every page were full. Locality: about one page change per page
    // used. Space: pages split at the inserted key run about half to two thirds
    // full (like B-tree leaves), and deleted rows leave holes (#19), so allow 2.5x.
    // Plain appends would change page on nearly every key.
    size_t packed = model.size() / 177 + 1;
    size_t pages  = t.heap_bytes() / BLOCK_SIZE;
    assert(switches <= pages + 5);
    assert(pages <= packed * 5 / 2);

    // A range scan returns exactly the model's rows in order.
    auto rows = t.range(be64(0), be64(1'000'000));
    assert(rows.size() == model.size());
    auto it = model.begin();
    for (const auto& r : rows) { assert(r[1] == str(it->second.c_str())); ++it; }
}

int main() {
    test_insert_and_lookup();
    test_duplicate_insert();
    test_missing_lookup();
    test_remove();
    test_multiple_rows();
    test_remove_and_reinsert();
    test_bloom_persisted_across_reopen();
    test_stale_bloom_rejected();
    test_missing_bloom_rebuilt();
    test_rows_clustered_by_key();
    cleanup();
    return 0;
}
