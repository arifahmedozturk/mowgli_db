#include "storage/disk_manager.h"
#include "index/chain.h"
#include <cassert>
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

int main() {
    test_create_and_reopen();
    test_alloc_and_readwrite();
    test_header_persistence();
    cleanup();
    return 0;
}
