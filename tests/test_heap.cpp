#include "storage/heap.h"
#include <cassert>
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

int main() {
    test_insert_and_read();
    test_multiple_records();
    test_remove();
    test_persist_and_reopen();
    test_many_records_cross_page();
    cleanup();
    return 0;
}
