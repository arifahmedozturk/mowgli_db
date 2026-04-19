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
    c.bit_phase    = 0;       // root chain: byte boundaries at split_bit % 8 == 0
    c.tail_record  = RecordPtr{};

    // split_bit=8: (0+8)%8==0 → byte boundary → record stored
    c.nodes.push_back({8,  3, 2, 42,        RecordPtr{77, 3}});
    // split_bit=13: (0+13)%8==5 → NOT byte boundary → record not stored
    c.nodes.push_back({13, 7, 4, NULL_BLOCK, RecordPtr{}});
    // split_bit=16: (0+16)%8==0 → byte boundary → record field stored (but null)
    c.nodes.push_back({16, 1, 0, 99,         RecordPtr{}});

    uint8_t buf[BLOCK_SIZE];
    assert(chain_encode(c, buf));

    ChainData out;
    assert(chain_decode(buf, out));

    assert(out.path_bit_len == 24);
    assert(out.bit_phase    == 0);
    assert(!out.tail_record.valid());
    assert(out.path_bits    == c.path_bits);
    assert(out.nodes.size() == 3);

    assert(out.nodes[0].split_bit          == 8);
    assert(out.nodes[0].light_child_weight == 3);
    assert(out.nodes[0].heavy_child_weight == 2);
    assert(out.nodes[0].light_child_block  == 42);
    assert(out.nodes[0].record.block_id    == 77);
    assert(out.nodes[0].record.slot        == 3);

    assert(out.nodes[1].split_bit         == 13);
    assert(out.nodes[1].light_child_block == NULL_BLOCK);
    assert(!out.nodes[1].record.valid());

    assert(out.nodes[2].split_bit         == 16);
    assert(out.nodes[2].light_child_block == 99);
    assert(!out.nodes[2].record.valid());
}

// Chain starting at bit 3 (bit_phase=3): byte boundaries where (3+split_bit)%8==0,
// i.e., split_bit % 8 == 5 → split_bit = 5, 13, 21, ...
static void test_roundtrip_nonzero_bit_phase() {
    ChainData c;
    c.path_bit_len = 16;
    c.bit_phase    = 3;
    c.path_bits    = {0b11001100, 0b10101010};
    c.tail_record  = RecordPtr{};

    // split_bit=5: (3+5)%8==0 → byte boundary → record stored
    c.nodes.push_back({5,  2, 1, 55, RecordPtr{100, 7}});
    // split_bit=7: (3+7)%8==2 → NOT byte boundary → no record
    c.nodes.push_back({7,  0, 0, NULL_BLOCK, RecordPtr{}});
    // split_bit=13: (3+13)%8==0 → byte boundary → record stored (null)
    c.nodes.push_back({13, 1, 1, 77, RecordPtr{}});

    uint8_t buf[BLOCK_SIZE];
    assert(chain_encode(c, buf));

    ChainData out;
    assert(chain_decode(buf, out));

    assert(out.bit_phase    == 3);
    assert(out.path_bit_len == 16);
    assert(out.nodes.size() == 3);

    assert(out.nodes[0].split_bit          == 5);
    assert(out.nodes[0].light_child_block  == 55);
    assert(out.nodes[0].record.block_id    == 100);
    assert(out.nodes[0].record.slot        == 7);

    assert(out.nodes[1].split_bit         == 7);
    assert(out.nodes[1].light_child_block == NULL_BLOCK);
    assert(!out.nodes[1].record.valid());

    assert(out.nodes[2].split_bit         == 13);
    assert(out.nodes[2].light_child_block == 77);
    assert(!out.nodes[2].record.valid());
}

// A record set on a non-byte-boundary node must NOT survive encode/decode:
// the caller is not supposed to do this, but verify the contract is enforced.
static void test_non_boundary_record_not_preserved() {
    ChainData c;
    c.path_bit_len = 16;
    c.bit_phase    = 0;
    c.path_bits    = {0b10101010, 0b11001100};
    c.tail_record  = RecordPtr{};

    // split_bit=5: (0+5)%8==5 → NOT a byte boundary
    c.nodes.push_back({5, 0, 0, NULL_BLOCK, RecordPtr{999, 1}});

    uint8_t buf[BLOCK_SIZE];
    assert(chain_encode(c, buf));

    ChainData out;
    assert(chain_decode(buf, out));

    // Record must have been dropped — lite node was serialised, no record field.
    assert(out.nodes.size() == 1);
    assert(out.nodes[0].split_bit == 5);
    assert(!out.nodes[0].record.valid());
}

// Round-trip via slice variants with a non-zero bit_phase.
static void test_slice_roundtrip_nonzero_phase() {
    ChainData c;
    c.path_bit_len = 8;
    c.bit_phase    = 6;   // byte boundary at (6+split_bit)%8==0 → split_bit%8==2
    c.path_bits    = {0b11110000};
    c.tail_record  = RecordPtr{};
    // split_bit=2: (6+2)%8==0 → full node with record
    c.nodes.push_back({2, 1, 0, 33, RecordPtr{42, 5}});

    uint8_t buf[512];
    size_t written = chain_encode_slice(c, buf, sizeof(buf));
    assert(written > 0);

    ChainData out;
    assert(chain_decode_slice(buf, written, out));

    assert(out.bit_phase == 6);
    assert(out.nodes.size() == 1);
    assert(out.nodes[0].split_bit         == 2);
    assert(out.nodes[0].record.block_id   == 42);
    assert(out.nodes[0].record.slot       == 5);
    assert(out.nodes[0].light_child_block == 33);
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
    test_roundtrip_nonzero_bit_phase();
    test_non_boundary_record_not_preserved();
    test_slice_roundtrip_nonzero_phase();
    return 0;
}
