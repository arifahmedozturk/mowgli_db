// Concurrent lookups and range scans on one table. Readers never change the
// hot chain cache (only writers, under the exclusive latch, do), so it has no
// lock of its own (#77); run under -fsanitize=thread to catch a reader that
// starts changing it.

#include "catalog/table.h"
#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include <atomic>
#include <cassert>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

static const char* TRIE_FILE = "/tmp/test_concurrent_reads.trie";
static const char* HEAP_FILE = "/tmp/test_concurrent_reads.heap";
static void cleanup() { std::remove(TRIE_FILE); std::remove(HEAP_FILE); }

static std::vector<uint8_t> key(uint64_t i) {
    std::string s = "k" + std::to_string(1'000'000 + i);
    return {s.begin(), s.end()};
}

// The same at the trie level with tiny buckets, so the trie is mostly chains
// (more than the hot cache holds) and a reader that filled the cache would be
// adding and evicting all the time while the others look things up.
static void trie_readers() {
    const char* file = "/tmp/test_concurrent_reads_trie.db";
    std::remove(file);
    constexpr uint64_t N = 30'000;
    auto k = [](uint64_t i) { auto v = key(i); return std::string(v.begin(), v.end()); };
    {
        auto dm = DiskManager::create(file);
        DiskTrie t(*dm, "", TrieOptions{128, 4});
        for (uint64_t i = 0; i < N; i++) t.insert(k(i), RecordPtr{i + 1, 0});
    }
    auto dm = DiskManager::open(file);
    DiskTrie t(*dm, "", TrieOptions{128, 4});
    assert(t.active_chain_count() > 13'500);
    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;
    for (int r = 0; r < 3; r++)
        threads.emplace_back([&, r] {
            for (uint64_t i = r; i < N; i += 3) {
                RecordPtr p;
                if (!t.lookup(k(i), &p) || p.block_id != i + 1) failed = true;
            }
        });
    for (int r = 0; r < 2; r++)
        threads.emplace_back([&, r] {
            for (uint64_t lo = r * 300; lo + 50 < N; lo += 600) {
                std::vector<std::pair<std::string, RecordPtr>> out;
                t.range_scan(k(lo), k(lo + 49), out);
                if (out.size() != 50) failed = true;
            }
        });
    for (auto& th : threads) th.join();
    assert(!failed);
    std::remove(file);
}

int main() {
    cleanup();
    Schema s{"t", {{"id", ColType::VARCHAR, 16}, {"v", ColType::VARCHAR, 8}}, 0};

    // Enough keys that the hot cache (13.5K chains) has to evict.
    constexpr uint64_t N = 40'000;
    {
        Table t = Table::create(s, TRIE_FILE, HEAP_FILE);
        std::vector<Row> rows;
        for (uint64_t i = 0; i < N; i++) rows.push_back({key(i), {'v'}});
        t.bulk_insert(std::move(rows));
    }

    // Reopen so the hot cache starts empty and readers populate it.
    Table t = Table::open(s, TRIE_FILE, HEAP_FILE);
    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;

    for (int r = 0; r < 4; r++)
        threads.emplace_back([&, r] {
            for (uint64_t i = r; i < N; i += 4)
                if (!t.lookup(key(i))) failed = true;
        });
    for (int r = 0; r < 2; r++)
        threads.emplace_back([&, r] {
            for (uint64_t lo = r * 500; lo + 100 < N; lo += 1000)
                if (t.range(key(lo), key(lo + 99)).size() != 100) failed = true;
        });
    for (auto& th : threads) th.join();

    assert(!failed);
    cleanup();
    trie_readers();
    std::puts("test_concurrent_reads: all passed");
    return 0;
}
