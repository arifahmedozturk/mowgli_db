// Concurrent lookups and range scans on one table. Range-scan cursors fill and
// evict the hot chain cache under a shared latch while lookups read it; run
// under -fsanitize=thread to catch unguarded hot-cache access.

#include "catalog/table.h"
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
    std::puts("test_concurrent_reads: all passed");
    return 0;
}
