// Concurrent inserts, lookups, range scans and removes on one Table. Writers
// race to insert the same keys, so the duplicate check and key count must be
// atomic per insert. Run under -fsanitize=thread to catch unguarded state.

#include "catalog/table.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <random>
#include <string>
#include <thread>
#include <vector>

static const char* TRIE_FILE = "/tmp/test_concurrent_writes.trie";
static const char* HEAP_FILE = "/tmp/test_concurrent_writes.heap";
static void cleanup() {
    std::remove(TRIE_FILE);
    std::remove(HEAP_FILE);
    std::remove((std::string(TRIE_FILE) + ".bloom").c_str());
}

static std::vector<uint8_t> key(uint64_t i) {
    std::string s = "k" + std::to_string(1'000'000 + i);
    return {s.begin(), s.end()};
}
static std::vector<uint8_t> val(uint64_t i) {
    std::string s = "v" + std::to_string(i);
    return {s.begin(), s.end()};
}

int main() {
    cleanup();
    Schema s{"t", {{"id", ColType::VARCHAR, 16}, {"v", ColType::VARCHAR, 16}}, 0};
    Table  t = Table::create(s, TRIE_FILE, HEAP_FILE);

    constexpr uint64_t N = 5'000;
    std::atomic<uint64_t> inserted{0};
    std::atomic<bool>     writing{true}, bad{false};

    // Phase 1: 4 writers insert all N keys (each in its own order) while
    // 2 readers look up and scan; every row a reader sees must be intact.
    std::vector<std::thread> writers, readers;
    for (int w = 0; w < 4; w++)
        writers.emplace_back([&, w] {
            std::vector<uint64_t> ids(N);
            for (uint64_t i = 0; i < N; i++) ids[i] = i;
            std::shuffle(ids.begin(), ids.end(), std::mt19937_64(w));
            for (uint64_t i : ids)
                if (t.insert({key(i), val(i)})) inserted++;
        });
    for (int r = 0; r < 2; r++)
        readers.emplace_back([&, r] {
            std::mt19937_64 rng(100 + r);
            while (writing) {
                uint64_t i = rng() % N;
                Row row;
                if (t.lookup(key(i), &row) && row[1] != val(i)) bad = true;
                for (const Row& rr : t.range(key(i), key(std::min(i + 20, N - 1))))
                    if (rr.size() != 2) bad = true;
                // The latch favours readers; yield so writers aren't starved.
                std::this_thread::yield();
            }
        });
    for (auto& th : writers) th.join();
    writing = false;
    for (auto& th : readers) th.join();

    assert(!bad);
    assert(inserted == N);           // each key won by exactly one writer
    assert(t.record_count() == N);   // no lost key-count updates
    for (uint64_t i = 0; i < N; i++) {
        Row row;
        assert(t.lookup(key(i), &row) && row[1] == val(i));
    }

    // Phase 2: 2 threads remove disjoint halves while a reader looks up.
    std::atomic<bool> removing{true};
    std::thread reader([&] {
        std::mt19937_64 rng(7);
        while (removing) {
            uint64_t i = rng() % N;
            Row row;
            if (t.lookup(key(i), &row) && row[1] != val(i)) bad = true;
            std::this_thread::yield();
        }
    });
    std::vector<std::thread> removers;
    for (int h = 0; h < 2; h++)
        removers.emplace_back([&, h] {
            for (uint64_t i = h; i < N; i += 2) if (!t.remove(key(i))) bad = true;
        });
    for (auto& th : removers) th.join();
    removing = false;
    reader.join();

    assert(!bad);
    assert(t.record_count() == 0);
    for (uint64_t i = 0; i < N; i++) assert(!t.lookup(key(i)));

    cleanup();
    std::puts("test_concurrent_writes: all passed");
    return 0;
}
