#include "storage/heap.h"
#include <algorithm>
#include <string>

// Placing a row whose key falls inside a full page (#67), like a B-tree leaf:
// shift the page's highest keys into the next page if that has real room,
// else split the two pages into three, else split the page at its median.
// Keys are views into the pages, so they are only read before rows move (#68).

HeapFile::SortedKeys HeapFile::sorted_keys(uint64_t page, const KeyOf& key_of) const {
    // Sort on the first 8 key bytes as a big-endian integer (the same order
    // as comparing the bytes), then the whole key on ties.
    struct Item { uint64_t head; std::string_view key; size_t bytes; };
    std::vector<Item> items;
    for_each_row(page, [&](const uint8_t* r, uint16_t n) {
        std::string_view k = key_of(r, n);
        uint64_t head = 0;
        for (size_t i = 0; i < 8; i++)
            head = head << 8 | (i < k.size() ? static_cast<uint8_t>(k[i]) : 0);
        items.push_back({head, k, size_t(n) + 4});   // the row and its slot entry
    });
    std::sort(items.begin(), items.end(), [](const Item& x, const Item& y) {
        return x.head != y.head ? x.head < y.head : x.key < y.key;
    });
    SortedKeys rows;
    rows.reserve(items.size());
    for (const auto& it : items) rows.push_back({it.key, it.bytes});
    return rows;
}

RecordPtr HeapFile::place_in_full_page(uint64_t page, std::string_view key, const uint8_t* data, uint16_t size,
                                       const KeyOf& key_of, std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    SortedKeys rows = sorted_keys(page, key_of);
    uint64_t   next = next_page(page);
    RecordPtr  r;
    if (next != NULL_BLOCK && free_bytes(next) >= BLOCK_SIZE / 8 &&
        (r = shift_into_next(page, next, rows, key, data, size, key_of, moved)).valid())
        return r;
    if (next != NULL_BLOCK &&
        (r = split_two_into_three(page, next, rows, key, data, size, key_of, moved)).valid())
        return r;
    return split_at_median(page, rows, key, data, size, key_of, moved);
}

// Move rows whose key passes pred from one page to another.
static void move_keys(HeapFile& heap, uint64_t from, uint64_t to, const HeapFile::KeyOf& key_of,
                      const std::function<bool(std::string_view)>& pred,
                      std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    heap.move_rows(from, to, [&](const uint8_t* r, uint16_t n) { return pred(key_of(r, n)); }, moved);
}

RecordPtr HeapFile::shift_into_next(uint64_t page, uint64_t next, const SortedKeys& rows, std::string_view key,
                                    const uint8_t* data, uint16_t size, const KeyOf& key_of,
                                    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    // The next page has real room: move this page's highest keys into it
    // until the two pages' free space is about even.
    size_t budget = free_bytes(next) / 2, moving = 0, i = rows.size();
    while (i > 1 && moving + rows[i - 1].second <= budget) moving += rows[--i].second;
    if (i == rows.size()) return RecordPtr{};
    const std::string first_moved(rows[i].first);
    move_keys(*this, page, next, key_of, [&](std::string_view k) { return k >= first_moved; }, moved);
    return insert_near(key >= first_moved ? next : page, NULL_BLOCK, data, size);
}

RecordPtr HeapFile::split_two_into_three(uint64_t page, uint64_t next, const SortedKeys& rows, std::string_view key,
                                         const uint8_t* data, uint16_t size, const KeyOf& key_of,
                                         std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    // Both full: split two pages into three (B*-tree), each about two thirds
    // full: a new page between them takes this page's top third and the next
    // page's bottom third. The next page's keys all follow this page's, so
    // the two sorted lists read as one.
    SortedKeys nrows = sorted_keys(next, key_of);
    auto at = [&](size_t i) -> const std::pair<std::string_view, size_t>& {
        return i < rows.size() ? rows[i] : nrows[i - rows.size()];
    };
    const size_t n = rows.size() + nrows.size();
    size_t total = 0;
    for (size_t i = 0; i < n; i++) total += at(i).second;
    size_t acc = 0, c1 = n, c2 = n;
    for (size_t i = 0; i < n; i++) {
        acc += at(i).second;
        if (c1 == n && acc > total / 3)     c1 = i;
        if (c2 == n && acc > total * 2 / 3) c2 = i;
    }
    if (!(c1 > 0 && c2 > c1 && c2 < n)) return RecordPtr{};
    const std::string lo(at(c1).first), hi(at(c2).first);   // middle page: keys in [lo, hi)
    uint64_t mid = new_page_after(page);
    move_keys(*this, page, mid, key_of, [&](std::string_view k) { return k >= lo; }, moved);
    move_keys(*this, next, mid, key_of, [&](std::string_view k) { return k < hi; }, moved);
    uint64_t target = key < lo ? page : key < hi ? mid : next;
    return insert_near(target, NULL_BLOCK, data, size);
}

RecordPtr HeapFile::split_at_median(uint64_t page, const SortedKeys& rows, std::string_view key,
                                    const uint8_t* data, uint16_t size, const KeyOf& key_of,
                                    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    // Both halves about half full; the row joins its side.
    const std::string median(rows[rows.size() / 2].first);
    uint64_t upper = split_page(page, [&](const uint8_t* r, uint16_t n) { return key_of(r, n) >= median; }, moved);
    return insert_near(key < median ? page : upper, NULL_BLOCK, data, size);
}
