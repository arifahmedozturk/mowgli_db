#include "index/chain.h"
#include <cassert>
#include <cstring>

static ChainData make(std::vector<uint8_t> path, uint8_t phase = 0) {
    ChainData c;
    c.path     = std::move(path);
    c.path_len = static_cast<uint16_t>(c.path.size());
    c.phase    = phase;
    return c;
}

static ChainNode node(uint16_t split, uint16_t heavy_w, std::vector<LightChild> lights,
                      RecordPtr rec = RecordPtr{}) {
    ChainNode n;
    n.split        = split;
    n.heavy_weight = heavy_w;
    n.lights       = std::move(lights);
    n.record       = rec;
    return n;
}

static ChainData roundtrip(const ChainData& c) {
    uint8_t buf[BLOCK_SIZE];
    assert(chain_encode(c, buf));
    ChainData out;
    assert(chain_decode(buf, out));
    return out;
}

static void test_roundtrip_empty_chain() {
    ChainData c = make({});
    c.tail_record = {42, 0};
    ChainData out = roundtrip(c);
    assert(out.path_len == 0);
    assert(out.tail_record.block_id == 42);
    assert(out.nodes.empty());
}

// Odd path length: the last byte holds a single (high) nibble.
static void test_roundtrip_odd_path() {
    ChainData c = make({0xA, 0x3, 0xF, 0x0, 0x7});
    c.tail_record = {9, 1};
    ChainData out = roundtrip(c);
    assert(out.path_len == 5);
    assert(out.path == c.path);
    assert(out.tail_record.block_id == 9 && out.tail_record.slot == 1);
}

static void test_roundtrip_with_nodes() {
    ChainData c = make({1, 2, 3, 4, 5, 6});
    // split 2 (even, phase 0): can hold a record; two lights.
    c.nodes.push_back(node(2, 3, {{0x0, 1, 100}, {0xE, 2, 101}}, RecordPtr{77, 3}));
    // split 3 (odd): no record field; one light.
    c.nodes.push_back(node(3, 7, {{0x9, 4, 102}}));
    // split 4 (even): record field present but empty; no lights.
    c.nodes.push_back(node(4, 1, {}));

    ChainData out = roundtrip(c);
    assert(out.path == c.path && out.phase == 0 && out.nodes.size() == 3);

    assert(out.nodes[0].split == 2 && out.nodes[0].heavy_weight == 3);
    assert(out.nodes[0].record.block_id == 77 && out.nodes[0].record.slot == 3);
    assert(out.nodes[0].lights.size() == 2);
    assert(out.nodes[0].lights[0].digit == 0x0 && out.nodes[0].lights[0].block == 100 &&
           out.nodes[0].lights[0].weight == 1);
    assert(out.nodes[0].lights[1].digit == 0xE && out.nodes[0].lights[1].block == 101 &&
           out.nodes[0].lights[1].weight == 2);

    assert(out.nodes[1].split == 3 && !out.nodes[1].record.valid());
    assert(out.nodes[1].lights.size() == 1 && out.nodes[1].lights[0].digit == 0x9 &&
           out.nodes[1].lights[0].block == 102);

    assert(out.nodes[2].split == 4 && out.nodes[2].lights.empty() && !out.nodes[2].record.valid());
}

// All 15 possible lights at one node (every digit but the heavy one).
static void test_full_fanout() {
    ChainData c = make({0x5, 0x1});
    std::vector<LightChild> lights;
    for (uint8_t d = 0; d < 16; d++)
        if (d != 0x5) lights.push_back({d, static_cast<uint16_t>(d), 1000u + d});
    c.nodes.push_back(node(0, 9, lights));
    ChainData out = roundtrip(c);
    assert(out.nodes.size() == 1 && out.nodes[0].lights.size() == 15);
    for (const auto& l : out.nodes[0].lights) {
        assert(l.digit != 0x5);
        assert(l.block == 1000u + l.digit && l.weight == l.digit);
    }
    assert(out.nodes[0].light_index(0x5) == -1 && out.nodes[0].light_index(0xF) == 14);
}

// phase 1: keys can end where (1 + split) is even, i.e. at odd splits.
static void test_roundtrip_phase_one() {
    ChainData c = make({3, 3, 3, 3}, 1);
    c.nodes.push_back(node(1, 0, {{0x8, 0, 55}}, RecordPtr{100, 7}));  // can hold a record
    c.nodes.push_back(node(2, 0, {{0x2, 0, 56}}));                      // cannot
    ChainData out = roundtrip(c);
    assert(out.phase == 1);
    assert(out.nodes[0].record.block_id == 100 && out.nodes[0].record.slot == 7);
    assert(!out.nodes[1].record.valid());
}

// A record on a node where no key can end is not stored.
static void test_non_boundary_record_not_preserved() {
    ChainData c = make({1, 2, 3, 4});
    c.nodes.push_back(node(1, 0, {}, RecordPtr{999, 1}));  // phase 0, odd split
    ChainData out = roundtrip(c);
    assert(out.nodes.size() == 1 && out.nodes[0].split == 1);
    assert(!out.nodes[0].record.valid());
}

static void test_slice_roundtrip() {
    ChainData c = make({0xF, 0x0}, 1);
    c.nodes.push_back(node(1, 1, {{0x3, 0, 33}}, RecordPtr{42, 5}));
    uint8_t buf[512];
    size_t written = chain_encode_slice(c, buf, sizeof(buf));
    assert(written > 0);
    ChainData out;
    assert(chain_decode_slice(buf, written, out));
    assert(out.phase == 1 && out.nodes.size() == 1);
    assert(out.nodes[0].record.block_id == 42 && out.nodes[0].lights[0].block == 33);

    // Too small a buffer to encode into; truncated bytes to decode from.
    assert(chain_encode_slice(c, buf, written - 1) == 0);
    assert(!chain_decode_slice(buf, written - 1, out));
}

// A chain whose encoding exceeds a block is rejected.
static void test_too_large_for_block() {
    ChainData c = make(std::vector<uint8_t>(600, 1));
    for (uint16_t s = 0; s < 600; s += 2) {
        std::vector<LightChild> lights;
        for (uint8_t d = 2; d < 4; d++) lights.push_back({d, 0, 7});
        c.nodes.push_back(node(s, 0, lights));
    }
    uint8_t buf[BLOCK_SIZE];
    assert(!chain_encode(c, buf));
}

static void test_bucket_roundtrip() {
    std::vector<BucketEntry> entries = {
        {{},              {1, 0}},   // the subtree's own key (empty suffix)
        {{0x0, 0x5},      {2, 1}},
        {{0x0, 0x5, 0xA}, {3, 2}},   // odd number of nibbles
        {{0x7},           {4, 3}},
        {{0xF, 0xF, 0xF}, {5, 4}},
    };
    ChainData b = make_bucket(entries, 1);
    assert(b.bucket && b.phase == 1 && bucket_size(b) == 5);
    assert(chain_encoded_size(b) == bucket_encoded_size(entries));

    // Encode / decode through both block and slice paths.
    ChainData out = roundtrip(b);
    assert(out.bucket && out.phase == 1 && out.bucket_bytes == b.bucket_bytes);
    uint8_t buf[512];
    size_t n = chain_encode_slice(b, buf, sizeof(buf));
    ChainData out2;
    assert(n == b.bucket_bytes.size() && chain_decode_slice(buf, n, out2) && out2.bucket);

    std::vector<BucketEntry> back;
    assert(bucket_decode(out, back) && back.size() == entries.size());
    for (size_t i = 0; i < entries.size(); i++) {
        assert(back[i].suffix == entries[i].suffix);
        assert(back[i].record.block_id == entries[i].record.block_id);
        assert(back[i].record.slot == entries[i].record.slot);
    }
    assert(bucket_lower_bound(out, {}) == 0);
    assert(bucket_lower_bound(out, {0x0, 0x5}) == 1);
    assert(bucket_lower_bound(out, {0x0, 0x6}) == 3);
    assert(bucket_lower_bound(out, {0x7, 0x0}) == 4);
    assert(bucket_lower_bound(out, {0xF, 0xF, 0xF, 0x0}) == 5);

    // Truncated or corrupted bytes are rejected.
    ChainData bad;
    assert(!chain_decode_slice(buf, n - 1, bad));
    buf[10] = 0xFF; buf[11] = 0x7F;   // first entry offset far outside the bucket
    assert(!chain_decode_slice(buf, n, bad));
}

static void test_bad_magic() {
    uint8_t buf[BLOCK_SIZE] = {};
    ChainData out;
    assert(!chain_decode(buf, out));
}

int main() {
    test_roundtrip_empty_chain();
    test_roundtrip_odd_path();
    test_roundtrip_with_nodes();
    test_full_fanout();
    test_roundtrip_phase_one();
    test_non_boundary_record_not_preserved();
    test_slice_roundtrip();
    test_too_large_for_block();
    test_bucket_roundtrip();
    test_bad_magic();
    return 0;
}
