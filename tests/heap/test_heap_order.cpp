// Heap pages hold close to contiguous key ranges (#65, #67): after random
// single inserts, which shift rows into the next page and split pages two
// into three or at the median, walking the keys in order changes page hardly
// more often than there are pages, so a range scan reads few pages. (Not
// exactly once per page: the next-page link a shift follows is a hint.)

#include "catalog/table.h"
#include "storage/heap.h"
#include <cassert>
#include <cstdio>
#include <map>
#include <random>
#include <string>
#include <vector>

static const char* TRIE_FILE = "/tmp/test_heap_order.trie";
static const char* HEAP_FILE = "/tmp/test_heap_order.heap";
static const std::string BLOOM_FILE = std::string(TRIE_FILE) + ".bloom";
static void cleanup() { std::remove(TRIE_FILE); std::remove(HEAP_FILE); std::remove(BLOOM_FILE.c_str()); }

static std::vector<uint8_t> be(uint64_t v) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; i++) b[i] = uint8_t(v >> (56 - 8 * i));
    return b;
}
static uint64_t key_of(const uint8_t* row) {   // the UINT64 primary key: the row's first 8 bytes
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = v << 8 | row[i];
    return v;
}

int main() {
    cleanup();
    Schema s{"t", {{"id", ColType::UINT64, 8}, {"v", ColType::VARCHAR, 120}}, 0};
    std::mt19937_64 rng(11);
    constexpr size_t N = 30000;
    {
        Table t = Table::create(s, TRIE_FILE, HEAP_FILE);
        for (size_t i = 0; i < N; i++)
            t.insert({be(rng()), std::vector<uint8_t>(10 + rng() % 110, 'x')});
    }

    // Each page's keys, and every key's page.
    HeapFile h = HeapFile::open(HEAP_FILE);
    std::map<uint64_t, uint64_t> page_of;
    std::map<uint64_t, size_t>   rows_on;
    for (uint64_t b = 1; b <= h.block_count(); b++)
        for (const auto& [bytes, ptr] : h.page_rows(b)) {
            page_of[key_of(bytes.data())] = b;
            rows_on[b]++;
        }
    assert(page_of.size() == N);
    assert(rows_on.size() > 100);   // many pages, so the shifts and splits ran

    // Runs of consecutive keys on the same page.
    size_t runs = 0;
    uint64_t cur = 0;
    for (const auto& [k, b] : page_of) if (b != cur) { runs++; cur = b; }
    assert(runs <= rows_on.size() + rows_on.size() / 20);
    // And the pages are well filled: rows are fixed-width (8 + 2 + 120 bytes,
    // plus a 4-byte slot), so no more than a third over the fewest pages that
    // could hold them (630 of 490 when written).
    const size_t min_pages = N * (8 + 2 + 120 + 4) / BLOCK_SIZE;
    assert(rows_on.size() <= min_pages * 4 / 3);
    cleanup();
    std::printf("test_heap_order: %zu keys on %zu pages in %zu key runs\n", N, rows_on.size(), runs);
    return 0;
}
