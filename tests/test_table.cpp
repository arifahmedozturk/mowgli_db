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
    // used (plain appends would change page on nearly every key). Space: with
    // shifts into the next page and 2-to-3 splits (#67) pages run ~3/4 full
    // (median splits alone: ~2/3, splits at the new key: ~1/2).
    size_t packed = model.size() / 177 + 1;
    size_t pages  = t.heap_bytes() / BLOCK_SIZE;
    assert(switches <= pages + 5);
    assert(pages <= packed * 3 / 2);

    // A range scan returns exactly the model's rows in order.
    auto rows = t.range(be64(0), be64(1'000'000));
    assert(rows.size() == model.size());
    auto it = model.begin();
    for (const auto& r : rows) { assert(r[1] == str(it->second.c_str())); ++it; }
}

// #19: a table that loses half its rows and gains as many new ones stays
// about the same size (deleted heap space and dead index slots are reused).
static void test_churn_reuses_space() {
    cleanup();
    auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);
    std::mt19937_64 rng(21);
    std::vector<uint64_t> keys;
    for (int i = 0; i < 20000; i++) {
        uint64_t k = rng();
        if (t.insert({be64(k), str("x")})) keys.push_back(k);
    }
    uint64_t heap0 = t.heap_bytes(), index0 = t.index_bytes();
    for (int round = 0; round < 3; round++) {
        std::vector<uint64_t> kept;
        for (size_t i = 0; i < keys.size(); i++) {
            if (i % 2) kept.push_back(keys[i]);
            else assert(t.remove(be64(keys[i])));
        }
        keys = kept;
        while (keys.size() < 20000) {
            uint64_t k = rng();
            if (t.insert({be64(k), str("x")})) keys.push_back(k);
        }
    }
    for (uint64_t k : keys) assert(t.lookup(be64(k)));
    assert(t.heap_bytes()  <= heap0  * 13 / 10);
    assert(t.index_bytes() <= index0 * 13 / 10);
}

// A BULK batch repeating keys stores each key once: the first row given wins,
// the count and the heap match a batch without the repeats.
static void test_bulk_insert_dedupes_batch() {
    const std::string pad(30, 'x');
    auto batch = [&](int copies) {
        std::vector<Row> rows;
        for (int c = 0; c < copies; c++)
            for (uint64_t i = 0; i < 3000; i++)
                rows.push_back({be64(i), str((std::to_string(c) + pad).c_str())});
        return rows;
    };

    cleanup();
    uint64_t heap_unique;
    {
        auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);
        assert(t.bulk_insert(batch(1)) == 3000);
        heap_unique = t.heap_bytes();
    }

    cleanup();
    auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);
    t.insert({be64(5000), str("single")});
    auto rows = batch(4);
    rows.push_back({be64(5000), str("dup of existing")});
    assert(t.bulk_insert(std::move(rows)) == 3000);
    assert(t.record_count() == 3001);
    assert(t.heap_bytes() <= heap_unique + BLOCK_SIZE);   // no leaked rows for the repeats

    for (uint64_t i = 0; i < 3000; i += 7) {
        Row out;
        assert(t.lookup(be64(i), &out));
        assert(out[1] == str(("0" + pad).c_str()));
    }
    Row out;
    assert(t.lookup(be64(5000), &out) && out[1] == str("single"));
    assert(t.range(be64(0), be64(UINT64_MAX)).size() == 3001);
}

// #67: deleting most rows merges sparse heap pages into their neighbours;
// every remaining row must still be found through its (repointed) record.
static void test_mass_delete_merges_pages() {
    cleanup();
    auto t = Table::create(make_schema(), TRIE_FILE, HEAP_FILE);
    std::mt19937_64 rng(31);
    std::map<uint64_t, std::string> model;
    while (model.size() < 20000) {
        uint64_t k = rng();
        std::string name = "m" + std::to_string(k % 9973);
        if (t.insert({be64(k), str(name.c_str())})) model[k] = name;
    }
    for (auto it = model.begin(); it != model.end();) {
        if (rng() % 10) { assert(t.remove(be64(it->first))); it = model.erase(it); }
        else ++it;
    }
    for (const auto& [k, name] : model) {
        Row out;
        assert(t.lookup(be64(k), &out) && out[1] == str(name.c_str()));
    }
    auto rows = t.range(be64(0), be64(UINT64_MAX));
    assert(rows.size() == model.size());
    // Freed pages are reused: refilling to the original size barely grows the heap.
    uint64_t heap = t.heap_bytes();
    while (model.size() < 20000) {
        uint64_t k = rng();
        if (t.insert({be64(k), str("r")})) model[k] = "r";
    }
    for (const auto& [k, name] : model) assert(t.lookup(be64(k)));
    assert(t.heap_bytes() <= heap + heap / 5);
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
    test_churn_reuses_space();
    test_bulk_insert_dedupes_batch();
    test_mass_delete_merges_pages();
    cleanup();
    return 0;
}
