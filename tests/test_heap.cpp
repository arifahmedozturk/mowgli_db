#include "storage/heap.h"
#include <cassert>
#include <random>
#include <cstdio>
#include <string>
#include <vector>

static const char* TEST_FILE = "/tmp/test_heap.db";
static void cleanup() { std::remove(TEST_FILE); }

static void test_insert_and_read() {
    cleanup();
    auto hf = HeapFile::create(TEST_FILE);

    std::string data = "hello world";
    RecordPtr ptr = hf.insert(
        reinterpret_cast<const uint8_t*>(data.data()),
        static_cast<uint16_t>(data.size()));

    std::vector<uint8_t> out;
    assert(hf.read(ptr, out));
    assert(std::string(out.begin(), out.end()) == data);
}

static void test_multiple_records() {
    cleanup();
    auto hf = HeapFile::create(TEST_FILE);

    std::vector<std::string> records = {"alpha", "beta", "gamma", "delta"};
    std::vector<RecordPtr> ptrs;
    for (const auto& r : records)
        ptrs.push_back(hf.insert(
            reinterpret_cast<const uint8_t*>(r.data()),
            static_cast<uint16_t>(r.size())));

    for (size_t i = 0; i < records.size(); i++) {
        std::vector<uint8_t> out;
        assert(hf.read(ptrs[i], out));
        assert(std::string(out.begin(), out.end()) == records[i]);
    }
}

static void test_remove() {
    cleanup();
    auto hf = HeapFile::create(TEST_FILE);

    std::string data = "to be deleted";
    RecordPtr ptr = hf.insert(
        reinterpret_cast<const uint8_t*>(data.data()),
        static_cast<uint16_t>(data.size()));

    hf.remove(ptr);

    std::vector<uint8_t> out;
    assert(!hf.read(ptr, out));
}

static void test_persist_and_reopen() {
    cleanup();
    RecordPtr ptr;
    {
        auto hf = HeapFile::create(TEST_FILE);
        std::string data = "persistent record";
        ptr = hf.insert(
            reinterpret_cast<const uint8_t*>(data.data()),
            static_cast<uint16_t>(data.size()));
    }
    {
        auto hf = HeapFile::open(TEST_FILE);
        std::vector<uint8_t> out;
        assert(hf.read(ptr, out));
        assert(std::string(out.begin(), out.end()) == "persistent record");
    }
}

static void test_many_records_cross_page() {
    cleanup();
    auto hf = HeapFile::create(TEST_FILE);

    // Insert enough records to span multiple 8KB pages.
    const int n = 500;
    std::vector<RecordPtr> ptrs;
    for (int i = 0; i < n; i++) {
        std::string data = "record_" + std::to_string(i);
        ptrs.push_back(hf.insert(
            reinterpret_cast<const uint8_t*>(data.data()),
            static_cast<uint16_t>(data.size())));
    }

    for (int i = 0; i < n; i++) {
        std::vector<uint8_t> out;
        assert(hf.read(ptrs[i], out));
        assert(std::string(out.begin(), out.end()) == "record_" + std::to_string(i));
    }
}

// insert_near: the hinted page while it has room, then a fresh page (not the
// shared append page); an invalid hint is a plain insert.
static void test_insert_near() {
    cleanup();
    auto hf = HeapFile::create(TEST_FILE);
    std::vector<uint8_t> row(100, 0xAB);

    RecordPtr first = hf.insert(row.data(), static_cast<uint16_t>(row.size()));
    RecordPtr near  = hf.insert_near(first.block_id, NULL_BLOCK, row.data(), static_cast<uint16_t>(row.size()));
    assert(near.block_id == first.block_id);

    // Fill that page through the hint; the overflow row starts a new page.
    RecordPtr r = near;
    while (r.block_id == first.block_id)
        r = hf.insert_near(first.block_id, NULL_BLOCK, row.data(), static_cast<uint16_t>(row.size()));
    uint64_t overflow_page = r.block_id;
    assert(overflow_page != first.block_id);

    // Plain inserts keep using their own append page, not the neighbour's new one.
    RecordPtr plain = hf.insert(row.data(), static_cast<uint16_t>(row.size()));
    assert(plain.block_id != overflow_page);

    RecordPtr none = hf.insert_near(NULL_BLOCK, NULL_BLOCK, row.data(), static_cast<uint16_t>(row.size()));
    assert(none.block_id == plain.block_id);

    // A full first choice falls back to the second page before starting a new one.
    RecordPtr alt = hf.insert_near(first.block_id, overflow_page, row.data(), static_cast<uint16_t>(row.size()));
    assert(alt.block_id == overflow_page);

    std::vector<uint8_t> out;
    for (RecordPtr p : {first, near, r, plain, none, alt}) {
        assert(hf.read(p, out));
        assert(out == row);
    }
}

// split_page moves the selected rows to a new page; the rest keep their slot
// numbers (so their pointers stay valid) and the page regains room.
static void test_split_page() {
    cleanup();
    auto hf = HeapFile::create(TEST_FILE);
    std::vector<RecordPtr> ptrs;
    for (uint8_t i = 0; ; i++) {               // fill one page with rows tagged 0, 1, 2, ...
        std::vector<uint8_t> row(100, i);
        RecordPtr p = hf.insert(row.data(), 100);
        if (!ptrs.empty() && p.block_id != ptrs[0].block_id) break;
        ptrs.push_back(p);
    }
    uint64_t page = ptrs[0].block_id;
    assert(!hf.has_room(page, 100));

    // Move rows tagged >= half.
    uint8_t half = static_cast<uint8_t>(ptrs.size() / 2);
    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> moved;
    uint64_t q = hf.split_page(page, [&](const uint8_t* r, uint16_t) { return r[0] >= half; }, moved);
    assert(q != NULL_BLOCK && q != page);
    assert(moved.size() == ptrs.size() - half);
    assert(hf.has_room(page, 100));

    std::vector<uint8_t> out;
    for (uint8_t i = 0; i < half; i++) {       // rows that stayed: same pointer, same bytes
        assert(hf.read(ptrs[i], out));
        assert(out == std::vector<uint8_t>(100, i));
    }
    for (const auto& [bytes, p] : moved) {     // moved rows: on the new page, intact
        assert(p.block_id == q && bytes[0] >= half);
        assert(hf.read(p, out) && out == bytes);
    }
    // The freed slots are reused by the next insert instead of growing the directory.
    std::vector<uint8_t> row(100, 0xEE);
    RecordPtr again = hf.insert_near(page, NULL_BLOCK, row.data(), 100);
    assert(again.block_id == page && again.slot >= half && again.slot < ptrs.size());
}

// #19: space of deleted rows is reclaimed by compacting the page in place;
// surviving rows keep their pointers, and churn doesn't grow the heap.
static void test_deleted_space_reused() {
    cleanup();
    auto hf = HeapFile::create(TEST_FILE);
    std::vector<RecordPtr> ptrs;
    for (uint8_t i = 0; ; i++) {
        std::vector<uint8_t> row(100, i);
        RecordPtr p = hf.insert(row.data(), 100);
        if (!ptrs.empty() && p.block_id != ptrs[0].block_id) { hf.remove(p); break; }
        ptrs.push_back(p);
    }
    uint64_t page = ptrs[0].block_id;
    assert(!hf.has_room(page, 100));
    for (size_t i = 0; i < ptrs.size(); i += 2) hf.remove(ptrs[i]);   // delete every other row
    assert(hf.has_room(page, 100));                                      // compaction would make room

    std::vector<uint8_t> fresh(100, 0xEE);
    for (size_t i = 0; i < ptrs.size() / 2; i++) {
        RecordPtr p = hf.insert_near(page, NULL_BLOCK, fresh.data(), 100);
        assert(p.block_id == page);                                      // reused, no new page
    }
    std::vector<uint8_t> out;
    for (size_t i = 1; i < ptrs.size(); i += 2) {                        // survivors intact
        assert(hf.read(ptrs[i], out));
        assert(out == std::vector<uint8_t>(100, static_cast<uint8_t>(i)));
    }

    // Churn through the append path: ~300 live rows, thousands of rounds.
    cleanup();
    auto h2 = HeapFile::create(TEST_FILE);
    std::vector<RecordPtr> live;
    std::mt19937 rng(9);
    std::vector<uint8_t> row(100, 0x11);
    uint64_t pages_after_warmup = 0;
    for (int round = 0; round < 20000; round++) {
        if (live.size() < 300 || rng() % 2) live.push_back(h2.insert(row.data(), 100));
        else {
            size_t i = rng() % live.size();
            h2.remove(live[i]);
            live[i] = live.back();
            live.pop_back();
        }
        if (round == 2000) pages_after_warmup = h2.block_count();
    }
    for (RecordPtr p : live) assert(h2.read(p, out) && out == row);
    assert(h2.block_count() <= pages_after_warmup + 3);
}

int main() {
    test_insert_and_read();
    test_multiple_records();
    test_remove();
    test_persist_and_reopen();
    test_many_records_cross_page();
    cleanup();
    test_split_page();
    test_deleted_space_reused();
    test_insert_near();
    return 0;
}
