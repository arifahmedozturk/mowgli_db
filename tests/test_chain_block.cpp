#include "index/chain.h"
#include <cassert>
#include <cstring>

static void test_roundtrip_empty_chain() {
    ChainData c;
    c.path_bit_len = 0;
    c.tail_record  = {42, 0};

    uint8_t buf[BLOCK_SIZE];
    assert(chain_encode(c, buf));

    ChainData out;
    assert(chain_decode(buf, out));
    assert(out.path_bit_len     == 0);
    assert(out.tail_record.block_id == 42);
    assert(out.nodes.empty());
}

static void test_roundtrip_with_nodes() {
    ChainData c;
    c.path_bits    = {0b10110010, 0b01101100, 0b11001010};
    c.path_bit_len = 24;
    c.tail_record  = RecordPtr{};

    // terminal branch at bit 5, with a record pointer
    c.nodes.push_back({5,  3, 2, 42,        RecordPtr{77, 3}});
    // no light child, no record
    c.nodes.push_back({13, 7, 4, NULL_BLOCK, RecordPtr{}});
    // light child, no record
    c.nodes.push_back({20, 1, 0, 99,         RecordPtr{}});

    uint8_t buf[BLOCK_SIZE];
    assert(chain_encode(c, buf));

    ChainData out;
    assert(chain_decode(buf, out));

    assert(out.path_bit_len         == 24);
    assert(!out.tail_record.valid());
    assert(out.path_bits            == c.path_bits);
    assert(out.nodes.size()         == 3);

    assert(out.nodes[0].split_bit          == 5);
    assert(out.nodes[0].light_child_weight == 3);
    assert(out.nodes[0].heavy_child_weight == 2);
    assert(out.nodes[0].light_child_block  == 42);
    assert(out.nodes[0].record.block_id    == 77);
    assert(out.nodes[0].record.slot        == 3);

    assert(out.nodes[1].light_child_block  == NULL_BLOCK);
    assert(!out.nodes[1].record.valid());

    assert(out.nodes[2].split_bit          == 20);
    assert(out.nodes[2].light_child_block  == 99);
}

static void test_bad_magic() {
    uint8_t buf[BLOCK_SIZE] = {};
    ChainData out;
    assert(!chain_decode(buf, out));
}

int main() {
    test_roundtrip_empty_chain();
    test_roundtrip_with_nodes();
    test_bad_magic();
    return 0;
}
