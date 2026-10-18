// Table::range_view and RowView (#64): a view's cells match what the row was
// inserted with, for every column type and position, including empty and
// full-length strings.

#include "catalog/table.h"
#include <cassert>
#include <cstdio>
#include <string>
#include <vector>

static const char* TRIE_FILE = "/tmp/test_row_view.trie";
static const char* HEAP_FILE = "/tmp/test_row_view.heap";
static const std::string BLOOM_FILE = std::string(TRIE_FILE) + ".bloom";
static void cleanup() { std::remove(TRIE_FILE); std::remove(HEAP_FILE); std::remove(BLOOM_FILE.c_str()); }

static std::vector<uint8_t> be(uint64_t v) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; i++) b[i] = uint8_t(v >> (56 - 8 * i));
    return b;
}

// Row i: (id, a string of i % 9 bytes, a number, a string of up to 4 bytes,
// a string of up to 300 bytes, so lengths need both length bytes).
static Row make_row(uint64_t i) {
    return {be(i), std::vector<uint8_t>(i % 9, uint8_t('a' + i % 26)),
            be(i * 1000003), std::vector<uint8_t>(i % 5, uint8_t('A' + i % 26)),
            std::vector<uint8_t>(i * 37 % 301, uint8_t('0' + i % 10))};
}

static std::vector<uint8_t> bytes(std::pair<const uint8_t*, size_t> c) { return {c.first, c.first + c.second}; }

int main() {
    cleanup();
    Schema s{"t", {{"id", ColType::UINT64, 8}, {"a", ColType::VARCHAR, 8},
                   {"n", ColType::UINT64, 8}, {"b", ColType::VARCHAR, 4},
                   {"c", ColType::VARCHAR, 300}}, 0};
    constexpr uint64_t N = 5000;
    Table t = Table::create(s, TRIE_FILE, HEAP_FILE);
    for (uint64_t i = 0; i < N; i++) t.insert(make_row(i));

    // Every row, and a range spanning more than one batch of record pointers.
    for (auto [lo, hi] : {std::pair<uint64_t, uint64_t>{0, N - 1}, {17, 4400}, {300, 300}}) {
        uint64_t next = lo;
        t.range_view(be(lo), be(hi), [&](const RowView& v) {
            Row want = make_row(next++);
            assert(v.columns() == want.size());
            for (size_t c = 0; c < want.size(); c++) assert(bytes(v.cell(c)) == want[c]);
            assert(v.to_row() == want);
        });
        assert(next == hi + 1);
    }

    // range and range_each are built on it and see the same rows.
    auto rows = t.range(be(10), be(19));
    assert(rows.size() == 10);
    for (uint64_t i = 0; i < 10; i++) assert(rows[i] == make_row(10 + i));

    // Nothing in an empty range.
    size_t seen = 0;
    t.range_view(be(N), be(N + 100), [&](const RowView&) { seen++; });
    assert(seen == 0);

    cleanup();
    std::puts("test_row_view: all passed");
    return 0;
}
