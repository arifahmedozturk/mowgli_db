#include "storage/heap.h"
#include <cassert>
#include <random>
#include <cstdio>
#include <string>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>
#include <map>

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

// #67: a sparse page absorbs its next page's rows (pointers reported for
// repointing), unlinks it, and the emptied page is reused by appends.
static void test_merge_next() {
    cleanup();
    auto hf = HeapFile::create(TEST_FILE);
    std::vector<uint8_t> row(100, 0x42);
    RecordPtr a = hf.insert(row.data(), 100);
    uint64_t p1 = a.block_id;
    uint64_t p2 = hf.new_page_after(p1);
    uint64_t p3 = hf.new_page_after(p2);
    assert(hf.next_page(p1) == p2 && hf.next_page(p2) == p3);
    std::vector<RecordPtr> in2;
    for (int i = 0; i < 10; i++) in2.push_back(hf.insert_near(p2, NULL_BLOCK, row.data(), 100));

    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> moved;
    assert(hf.merge_next(p1, moved));
    assert(moved.size() == 10);
    for (const auto& [bytes, p] : moved) {
        std::vector<uint8_t> out;
        assert(p.block_id == p1 && hf.read(p, out) && out == bytes);
    }
    assert(hf.next_page(p1) == p3);   // p2 unlinked
    // p2 is empty now and gets reused by plain appends once the append page fills.
    bool reused = false;
    for (int i = 0; i < 200 && !reused; i++) reused = hf.insert(row.data(), 100).block_id == p2;
    assert(reused);

    // Not sparse: no merge.
    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> none;
    for (int i = 0; i < 40; i++) hf.insert_near(p3, NULL_BLOCK, row.data(), 100);
    for (int i = 0; i < 40; i++) hf.insert_near(p1, NULL_BLOCK, row.data(), 100);
    assert(!hf.merge_next(p1, none) && none.empty());
}

// Random mix of every heap operation against a model of the live rows: after
// each round every row reads back unchanged and every page's live-byte and
// dead-slot counts in its header match its slot directory (#74).
static void test_page_counts_model() {
    cleanup();
    auto hf = HeapFile::create(TEST_FILE);
    std::mt19937_64 rng(74);
    std::map<uint64_t, RecordPtr> where;          // row id -> location
    std::map<uint64_t, std::vector<uint8_t>> rows;
    uint64_t next_id = 1;
    auto make_row = [&](uint64_t id) {
        std::vector<uint8_t> r(8 + rng() % 120);
        for (size_t i = 0; i < 8; i++) r[i] = static_cast<uint8_t>(id >> (8 * i));
        for (size_t i = 8; i < r.size(); i++) r[i] = static_cast<uint8_t>(rng());
        return r;
    };
    auto id_of = [](const uint8_t* r) { uint64_t id = 0; for (size_t i = 0; i < 8; i++) id |= uint64_t(r[i]) << (8 * i); return id; };
    auto apply_moves = [&](const std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
        for (const auto& [bytes, p] : moved) where[id_of(bytes.data())] = p;
    };
    auto random_live = [&]() { auto it = where.begin(); std::advance(it, static_cast<long>(rng() % where.size())); return it; };

    for (int round = 0; round < 200; round++) {
        for (int k = 0; k < 50; k++) {
            int op = static_cast<int>(rng() % 10);
            if (op < 4 || where.empty()) {
                uint64_t id = next_id++;
                auto r = make_row(id);
                RecordPtr near = where.empty() ? RecordPtr{} : random_live()->second;
                RecordPtr p = (op % 2 || where.empty()) ? hf.insert(r.data(), static_cast<uint16_t>(r.size()))
                                                       : hf.insert_near(near.block_id, NULL_BLOCK, r.data(), static_cast<uint16_t>(r.size()));
                where[id] = p; rows[id] = r;
            } else if (op < 7) {
                auto it = random_live();
                hf.remove(it->second);
                rows.erase(it->first); where.erase(it);
            } else if (op == 7) {
                std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> moved;
                uint64_t page = random_live()->second.block_id;
                hf.split_page(page, [&](const uint8_t* r, uint16_t) { return id_of(r) % 2 == 0; }, moved);
                apply_moves(moved);
            } else if (op == 8) {
                std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> moved;
                uint64_t from = random_live()->second.block_id, to = random_live()->second.block_id;
                if (from != to) hf.move_rows(from, to, [&](const uint8_t* r, uint16_t) { return id_of(r) % 3 == 0; }, moved);
                apply_moves(moved);
            } else {
                std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> moved;
                hf.merge_next(random_live()->second.block_id, moved);
                apply_moves(moved);
            }
        }
        assert(hf.check_page_counts());
        for (const auto& [id, p] : where) {
            std::vector<uint8_t> out;
            assert(hf.read(p, out) && out == rows[id]);
        }
    }
    assert(where.size() > 100);
}

// #27: a crash (here: a child process that exits without running any
// destructor, so the heap is never closed) must not lose the allocation
// state, or the reopened heap hands out pages that already hold rows.
static void test_allocation_survives_crash() {
    cleanup();
    { auto hf = HeapFile::create(TEST_FILE); }
    std::vector<std::pair<RecordPtr, std::vector<uint8_t>>> written;
    int fds[2];
    assert(::pipe(fds) == 0);
    pid_t pid = ::fork();
    if (pid == 0) {
        ::close(fds[0]);
        auto hf = HeapFile::open(TEST_FILE);
        for (uint32_t i = 0; i < 2000; i++) {   // ~20 pages, plus near-inserts onto old ones
            std::vector<uint8_t> row(60 + i % 40, static_cast<uint8_t>(i));
            RecordPtr p = (i % 3) ? hf.insert(row.data(), static_cast<uint16_t>(row.size()))
                                  : hf.insert_near(1, NULL_BLOCK, row.data(), static_cast<uint16_t>(row.size()));
            uint64_t rec[2] = {p.block_id, (uint64_t(p.slot) << 32) | i};
            if (::write(fds[1], rec, sizeof(rec)) != sizeof(rec)) ::_exit(2);
        }
        ::_exit(0);   // no destructors: the heap is not closed
    }
    ::close(fds[1]);
    uint64_t rec[2];
    while (::read(fds[0], rec, sizeof(rec)) == sizeof(rec)) {
        uint32_t i = static_cast<uint32_t>(rec[1]);
        written.push_back({RecordPtr{rec[0], static_cast<uint16_t>(rec[1] >> 32)},
                           std::vector<uint8_t>(60 + i % 40, static_cast<uint8_t>(i))});
    }
    ::close(fds[0]);
    int status = 0;
    ::waitpid(pid, &status, 0);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0 && written.size() == 2000);

    auto hf = HeapFile::open(TEST_FILE);
    for (int i = 0; i < 3000; i++) {   // new pages after the crash
        std::vector<uint8_t> row(90, 0xEE);
        hf.insert(row.data(), static_cast<uint16_t>(row.size()));
    }
    for (const auto& [p, want] : written) {
        std::vector<uint8_t> got;
        assert(hf.read(p, got) && got == want);
    }
    assert(hf.check_page_counts());
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
    test_merge_next();
    test_insert_near();
    test_page_counts_model();
    test_allocation_survives_crash();
    return 0;
}
