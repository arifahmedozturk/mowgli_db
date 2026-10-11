#include "storage/disk_manager.h"
#include "index/chain.h"
#include <algorithm>
#include <cassert>
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
    c.path_bits    = {0xAB, 0xCD};
    c.path_bit_len = 16;
    c.tail_record  = RecordPtr{99, 0};
    c.nodes.push_back({8, 5, 0, b1, RecordPtr{}});

    uint8_t wbuf[BLOCK_SIZE];
    assert(chain_encode(c, wbuf));
    dm.write_block(b0, wbuf);

    uint8_t rbuf[BLOCK_SIZE];
    dm.read_block(b0, rbuf);

    ChainData out;
    assert(chain_decode(rbuf, out));
    assert(out.path_bit_len            == 16);
    assert(out.tail_record.block_id    == 99);
    assert(out.nodes.size()            == 1);
    assert(out.nodes[0].split_bit      == 8);
    assert(out.nodes[0].light_child_block == b1);
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

int main() {
    test_create_and_reopen();
    test_alloc_and_readwrite();
    test_header_persistence();
    test_large_free_list_persisted();
    cleanup();
    return 0;
}
