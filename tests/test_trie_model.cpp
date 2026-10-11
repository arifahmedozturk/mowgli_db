// Randomized model test: a long sequence of random operations on DiskTrie is
// checked against std::map after every step batch. Keys come from a small byte
// alphabet with varied lengths, so many keys share prefixes or are prefixes of
// each other, and both nibble phases occur.

#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

static const char* TEST_FILE = "/tmp/test_trie_model.db";

static std::string random_key(std::mt19937_64& rng) {
    static const unsigned char ALPHA[] = {0x00, 0x01, 0x0F, 0x10, 0x11, 0x7F, 0x80, 0xF0, 0xFF, 'a', 'b'};
    std::uniform_int_distribution<int> len(1, 12), pick(0, sizeof(ALPHA) - 1);
    std::string k(len(rng), '\0');
    for (char& c : k) c = static_cast<char>(ALPHA[pick(rng)]);
    return k;
}

using Model = std::map<std::string, uint64_t>;

static void check(DiskTrie& t, const Model& m, std::mt19937_64& rng) {
    for (const auto& [k, v] : m) {
        RecordPtr p;
        assert(t.lookup(k, &p));
        assert(p.block_id == v);
    }
    for (int i = 0; i < 50; i++) {
        std::string k = random_key(rng);
        assert(t.lookup(k) == (m.count(k) == 1));
    }

    // Full scan in order.
    std::vector<std::pair<std::string, RecordPtr>> all;
    t.range_scan(std::string(), std::string(16, '\xFF'), all);
    assert(all.size() == m.size());
    size_t i = 0;
    for (const auto& [k, v] : m) {
        assert(all[i].first == k && all[i].second.block_id == v);
        i++;
    }

    // Random ranges and lower bounds.
    for (int r = 0; r < 20; r++) {
        std::string lo = random_key(rng), hi = random_key(rng);
        if (hi < lo) std::swap(lo, hi);
        std::vector<std::pair<std::string, RecordPtr>> got;
        t.range_scan(lo, hi, got);
        auto it = m.lower_bound(lo);
        for (const auto& [k, rec] : got) {
            assert(it != m.end() && it->first == k && it->second == rec.block_id);
            ++it;
        }
        assert(it == m.end() || it->first > hi);

        TrieCursor c;
        auto lb = m.lower_bound(lo);
        bool has = t.lower_bound(lo, c);
        assert(has == (lb != m.end()));
        if (has) {
            std::string k;
            RecordPtr rec;
            for (int s = 0; s < 5 && lb != m.end(); s++, ++lb) {
                assert(t.cursor_next(c, k, rec));
                assert(k == lb->first && rec.block_id == lb->second);
            }
        }
    }
}

// One full run of random operations with the given trie options.
static size_t bursts = 0, merges = 0;

static size_t run(TrieOptions opts, uint64_t seed) {
    std::remove(TEST_FILE);
    std::mt19937_64 rng(seed);
    Model m;
    uint64_t next_id = 1;

    auto dm = DiskManager::create(TEST_FILE);
    auto t  = std::make_unique<DiskTrie>(*dm, "", opts);

    for (int round = 0; round < 60; round++) {
        int op = static_cast<int>(rng() % 10);
        if (op < 5) {
            // Single inserts, including duplicates.
            for (int i = 0; i < 200; i++) {
                std::string k = random_key(rng);
                bool fresh = !m.count(k);
                t->insert(k, RecordPtr{next_id, 0});
                if (fresh) m[k] = next_id;
                next_id++;
            }
        } else if (op < 7 && !m.empty()) {
            // Removes: existing and missing keys.
            for (int i = 0; i < 100 && !m.empty(); i++) {
                auto it = m.begin();
                std::advance(it, static_cast<long>(rng() % m.size()));
                assert(t->remove(it->first));
                m.erase(it);
                std::string k = random_key(rng);
                assert(t->remove(k) == (m.erase(k) == 1));
            }
        } else if (op == 7) {
            // Bulk insert into a non-empty trie (sorted, may contain existing keys).
            std::vector<std::pair<std::string, RecordPtr>> kvs;
            std::map<std::string, uint64_t> batch;
            for (int i = 0; i < 300; i++) batch.emplace(random_key(rng), next_id++);
            for (const auto& [k, v] : batch) {
                kvs.push_back({k, RecordPtr{v, 0}});
                m.emplace(k, v);   // existing keys keep their old value
            }
            t->bulk_insert(std::move(kvs));
        } else if (op == 8) {
            if (rng() % 2) t->compact(); else t->compact_lex();
        } else if (rng() % 2 && m.size() > 400) {
            // Mass delete: shrink subtrees enough to merge chains back into buckets.
            for (auto it = m.begin(); it != m.end();) {
                if (rng() % 4) { assert(t->remove(it->first)); it = m.erase(it); }
                else ++it;
            }
        } else {
            // Reopen: fresh trie (lazy counts, saved/rebuilt Bloom) on a reopened file.
            bursts += t->bucket_bursts();
            merges += t->bucket_merges();
            t.reset();
            dm.reset();
            dm = DiskManager::open(TEST_FILE);
            t  = std::make_unique<DiskTrie>(*dm, "", opts);
        }
        check(*t, m, rng);
    }
    assert(m.size() > 200);
    bursts += t->bucket_bursts();
    merges += t->bucket_merges();

    t.reset();
    dm.reset();
    std::remove(TEST_FILE);
    return m.size();
}

int main() {
    // Default buckets (up to a block), and tiny buckets so that bursts and
    // merges back into buckets happen all the time.
    size_t a = run(TrieOptions{}, 2024);
    size_t b = run(TrieOptions{128, 4}, 7);
    size_t c = run(TrieOptions{256, 16}, 99);
    assert(bursts > 100 && merges > 10);   // the small-bucket runs really burst and merge
    std::printf("test_trie_model: %zu / %zu / %zu keys, %zu bursts, %zu merges, all checks passed\n",
                a, b, c, bursts, merges);
    return 0;
}
