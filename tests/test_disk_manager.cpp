#include "storage/disk_manager.h"
#include "index/chain.h"
#include <algorithm>
#include <cassert>
#include <random>
#include <set>
#include <vector>
#include <cstring>
#include <cstdio>

static const char* TEST_FILE = "/tmp/test_heavy_trie.db";

static void cleanup() { std::remove(TEST_FILE); }

static void test_create_and_reopen() {
    cleanup();
    {
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        assert(dm.root_block() == NULL_BLOCK);
        assert(dm.key_count()  == 0);
    }
    {
        auto dm_ptr = DiskManager::open(TEST_FILE); auto& dm = *dm_ptr;
        assert(dm.root_block() == NULL_BLOCK);
        assert(dm.key_count()  == 0);
    }
}

static void test_alloc_and_readwrite() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;

    uint64_t b0 = dm.alloc_block();
    uint64_t b1 = dm.alloc_block();
    assert(b0 == 1);
    assert(b1 == 2);

    // Write a chain to b0, read it back.
    ChainData c;
    c.path        = {0xA, 0xB, 0xC, 0xD};
    c.path_len    = 4;
    c.tail_record = RecordPtr{99, 0};
    ChainNode n;
    n.split        = 2;
    n.heavy_weight = 5;
    n.lights       = {{0x3, 0, b1}};
    c.nodes.push_back(n);

    uint8_t wbuf[BLOCK_SIZE];
    assert(chain_encode(c, wbuf));
    dm.write_block(b0, wbuf);

    uint8_t rbuf[BLOCK_SIZE];
    dm.read_block(b0, rbuf);

    ChainData out;
    assert(chain_decode(rbuf, out));
    assert(out.path_len             == 4);
    assert(out.path                 == c.path);
    assert(out.tail_record.block_id == 99);
    assert(out.nodes.size()         == 1);
    assert(out.nodes[0].split       == 2);
    assert(out.nodes[0].heavy_weight == 5);
    assert(out.nodes[0].lights.size() == 1 && out.nodes[0].lights[0].block == b1);
}

static void test_header_persistence() {
    cleanup();
    {
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        uint64_t root = dm.alloc_block();
        dm.set_root_block(root);
        dm.set_key_count(42);
    }
    {
        auto dm_ptr = DiskManager::open(TEST_FILE); auto& dm = *dm_ptr;
        assert(dm.root_block() == 1);
        assert(dm.key_count()  == 42);
    }
}

// More free blocks than fit in the header block must all survive close/reopen
// (via overflow pages) and be handed out again before any new block.
static void test_large_free_list_persisted() {
    cleanup();
    const size_t ALLOC = 5000, FREED = 3000;  // the header holds ~1019 IDs
    std::set<uint64_t> freed;
    uint64_t high_water;
    {
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        std::vector<uint64_t> ids;
        for (size_t i = 0; i < ALLOC; i++) ids.push_back(dm.alloc_block());
        high_water = *std::max_element(ids.begin(), ids.end());
        for (size_t i = 0; i < FREED; i++) {
            uint64_t id = ids[i * ALLOC / FREED];
            dm.free_block(id);
            freed.insert(id);
        }
    }
    // Reopen twice: the second open must not lose what the first one read back.
    { auto dm_ptr = DiskManager::open(TEST_FILE); }
    auto dm_ptr = DiskManager::open(TEST_FILE); auto& dm = *dm_ptr;
    std::set<uint64_t> reused;
    for (size_t i = 0; i < FREED; i++) {
        uint64_t id = dm.alloc_block();
        assert(id <= high_water);           // a recycled block, not a new one
        assert(reused.insert(id).second);   // never handed out twice
    }
    assert(reused == freed);
    assert(dm.alloc_block() > high_water);  // free list now exhausted
}

static ChainData chain_of(size_t nibbles, uint8_t seed) {
    ChainData c;
    for (size_t i = 0; i < nibbles; i++) c.path.push_back(static_cast<uint8_t>((i * 7 + seed) & 0x0F));
    c.path_len    = static_cast<uint16_t>(nibbles);
    c.tail_record = RecordPtr{seed, 0};
    return c;
}

static void assert_chain(DiskManager& dm, uint64_t addr, size_t nibbles, uint8_t seed) {
    ChainData out = dm.read_chain_at(addr);
    assert(out.path_len == nibbles && out.path == chain_of(nibbles, seed).path);
    assert(out.tail_record.block_id == seed);
}

// A chain grows through every packed size class and past PACK_THRESHOLD into
// a dedicated block, then shrinks: its original address keeps working (one
// forwarding stub, repointed on each move), and freeing it releases everything.
static void test_slot_classes_grow_and_move() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    uint64_t addr = dm.alloc_chain_slot(chain_of(10, 1));
    assert(chain_addr_slot(addr) != 0);              // small: packed
    for (size_t n : {10, 100, 300, 700, 1500, 3000, 6000, 8500, 12000, 200, 20}) {
        dm.update_chain_at(addr, chain_of(n, 1));
        assert_chain(dm, addr, n, 1);
        DiskManager::ChainBytes cb = dm.pin_chain_bytes(addr);   // zero-copy path follows the stub too
        dm.unpin_chain_shared(cb.pinned_block);
    }
    // A neighbour in the same packed block is untouched by all the moves.
    uint64_t other = dm.alloc_chain_slot(chain_of(30, 2));
    assert_chain(dm, other, 30, 2);

    dm.free_chain_slot(addr);
    dm.free_chain_slot(other);

    // Everything is freed on each move and on free: repeating the whole
    // grow/move/free cycle must not need more blocks (leaked slots would pile up).
    uint64_t blocks = 0;
    for (int round = 0; round < 50; round++) {
        uint64_t a = dm.alloc_chain_slot(chain_of(10, 3));
        uint64_t b = dm.alloc_chain_slot(chain_of(20, 4));
        for (size_t n : {100, 300, 700, 1500, 3000, 6000, 12000}) dm.update_chain_at(a, chain_of(n, 3));
        assert_chain(dm, a, 12000, 3);
        assert_chain(dm, b, 20, 4);
        dm.free_chain_slot(a);
        dm.free_chain_slot(b);
        if (round == 0) blocks = dm.chain_count();
        assert(dm.chain_count() == blocks);
    }
}

// Many chains of mixed sizes share packed blocks; all survive a reopen
// (which rebuilds the packed-block state from the directories).
static void test_mixed_classes_reopen() {
    cleanup();
    std::vector<std::pair<uint64_t, size_t>> chains;
    {
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        for (size_t i = 0; i < 400; i++) {
            size_t n = 4 + (i * 37) % 900;          // 21 B .. ~470 B encoded
            chains.push_back({dm.alloc_chain_slot(chain_of(n, static_cast<uint8_t>(i))), n});
        }
        for (size_t i = 0; i < chains.size(); i += 3) {   // grow a third of them past their class
            chains[i].second += 700;
            dm.update_chain_at(chains[i].first, chain_of(chains[i].second, static_cast<uint8_t>(i)));
        }
    }
    auto dm_ptr = DiskManager::open(TEST_FILE); auto& dm = *dm_ptr;
    for (size_t i = 0; i < chains.size(); i++)
        assert_chain(dm, chains[i].first, chains[i].second, static_cast<uint8_t>(i));
    // A new chain packs into the reopened blocks without disturbing the others.
    uint64_t extra = dm.alloc_chain_slot(chain_of(50, 99));
    assert_chain(dm, extra, 50, 99);
    for (size_t i = 0; i < chains.size(); i++)
        assert_chain(dm, chains[i].first, chains[i].second, static_cast<uint8_t>(i));
}

// #19: a freed packed slot's region is reused in place, so churn doesn't grow
// the file; dead regions are still reusable after a reopen.
static void test_dead_slots_reused() {
    cleanup();
    uint64_t block;
    std::vector<uint64_t> addrs;
    {
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        for (int i = 0; i < 20; i++) addrs.push_back(dm.alloc_chain_slot(chain_of(40, static_cast<uint8_t>(i))));
        block = chain_addr_phys(addrs[0]);
        assert(chain_addr_phys(addrs[19]) == block);            // all in one packed block
        dm.free_chain_slot(addrs[5]);
        uint64_t again = dm.alloc_chain_slot(chain_of(40, 77), block);
        assert(again == addrs[5]);                              // same block, same slot
        assert_chain(dm, again, 40, 77);
        for (int i = 0; i < 20; i++) if (i != 5) assert_chain(dm, addrs[i], 40, static_cast<uint8_t>(i));
        dm.free_chain_slot(addrs[7]);
        dm.free_chain_slot(addrs[8]);
    }
    {   // after a reopen the dead regions are found again
        auto dm_ptr = DiskManager::open(TEST_FILE); auto& dm = *dm_ptr;
        uint64_t a = dm.alloc_chain_slot(chain_of(40, 88), block);
        uint64_t b = dm.alloc_chain_slot(chain_of(40, 89), block);
        assert(chain_addr_phys(a) == block && chain_addr_phys(b) == block);
        assert((a == addrs[7] && b == addrs[8]) || (a == addrs[8] && b == addrs[7]));
        assert_chain(dm, a, 40, 88);
        assert_chain(dm, b, 40, 89);
    }

    // Churn: ~200 live chains of mixed sizes, thousands of alloc/free rounds.
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    std::mt19937 rng(5);
    std::vector<std::pair<uint64_t, size_t>> live;
    uint64_t peak_after_warmup = 0;
    for (int round = 0; round < 6000; round++) {
        if (live.size() < 200 || rng() % 2) {
            size_t n = 4 + rng() % 400;   // ~21 B .. ~220 B: several size classes
            live.push_back({dm.alloc_chain_slot(chain_of(n, static_cast<uint8_t>(n))), n});
        } else {
            size_t i = rng() % live.size();
            dm.free_chain_slot(live[i].first);
            live[i] = live.back();
            live.pop_back();
        }
        if (round == 1000) peak_after_warmup = dm.chain_count();
    }
    for (const auto& [addr, n] : live) assert_chain(dm, addr, n, static_cast<uint8_t>(n));
    // Without reuse every allocation would eventually take fresh space.
    assert(dm.chain_count() <= peak_after_warmup + 4);
}

int main() {
    test_create_and_reopen();
    test_alloc_and_readwrite();
    test_header_persistence();
    test_large_free_list_persisted();
    test_slot_classes_grow_and_move();
    test_mixed_classes_reopen();
    test_dead_slots_reused();
    cleanup();
    return 0;
}
