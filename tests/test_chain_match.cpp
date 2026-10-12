#include "index/chain.h"
#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include <cassert>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

static RecordPtr SOME_RECORD{42, 0};

static uint8_t hexval(char c) { return static_cast<uint8_t>(c <= '9' ? c - '0' : c - 'A' + 10); }

// Chain whose path is the given hex digits (one nibble each).
static ChainData make_chain(const char* hex, bool tail_has_record = false, uint8_t phase = 0) {
    ChainData c;
    for (const char* p = hex; *p; p++) c.path.push_back(hexval(*p));
    c.path_len    = static_cast<uint16_t>(c.path.size());
    c.phase       = phase;
    c.tail_record = tail_has_record ? SOME_RECORD : RecordPtr{};
    return c;
}

// Key bytes from an even number of hex digits.
static std::string make_key(const char* hex) {
    std::string h(hex), key;
    for (size_t i = 0; i + 1 < h.size(); i += 2)
        key.push_back(static_cast<char>(hexval(h[i]) << 4 | hexval(h[i + 1])));
    return key;
}

static ChainNode branch(uint16_t split, std::vector<LightChild> lights, RecordPtr rec = RecordPtr{}) {
    ChainNode n;
    n.split  = split;
    n.lights = std::move(lights);
    n.record = rec;
    return n;
}

static void test_exact_match_found() {
    auto r = chain_match(make_chain("B2", true), make_key("B2"), 0);
    assert(r.kind == MatchOutcome::Kind::FOUND && r.record.valid());
}

static void test_exact_match_miss() {
    auto r = chain_match(make_chain("B2", true), make_key("B3"), 0);
    assert(r.kind == MatchOutcome::Kind::MISS);
}

// At a node, the key's digit picks the light child with that digit.
static void test_follow_light() {
    auto c = make_chain("B2A0");
    c.nodes.push_back(branch(2, {{0x1, 0, 91}, {0xC, 0, 99}}));

    auto r = chain_match(c, make_key("B2C0"), 0);   // digit 2 = C, chain has A
    assert(r.kind == MatchOutcome::Kind::FOLLOW_LIGHT);
    assert(r.node_index == 0 && r.next_key_pos == 3 && r.light_child == 99);

    r = chain_match(c, make_key("B210"), 0);
    assert(r.kind == MatchOutcome::Kind::FOLLOW_LIGHT && r.light_child == 91);

    // No light child for digit 7 at that node.
    assert(chain_match(c, make_key("B270"), 0).kind == MatchOutcome::Kind::MISS);
    // Mismatch where there is no node at all.
    assert(chain_match(c, make_key("B3A0"), 0).kind == MatchOutcome::Kind::MISS);
}

// A key ending at a node is found only if that node has a record.
static void test_key_ends_at_node() {
    auto c = make_chain("B2A0");
    c.nodes.push_back(branch(2, {{0x1, 0, 91}}, SOME_RECORD));
    auto r = chain_match(c, make_key("B2"), 0);
    assert(r.kind == MatchOutcome::Kind::FOUND && r.record.block_id == SOME_RECORD.block_id);

    c.nodes[0].record = RecordPtr{};
    assert(chain_match(c, make_key("B2"), 0).kind == MatchOutcome::Kind::MISS);
}

static void test_key_shorter_than_chain_without_node() {
    assert(chain_match(make_chain("B2A0", true), make_key("B2"), 0).kind == MatchOutcome::Kind::MISS);
}

static void test_key_extends_past_chain() {
    assert(chain_match(make_chain("B2", true), make_key("B2A0"), 0).kind == MatchOutcome::Kind::MISS);
    assert(chain_match(make_chain("B2", false), make_key("B2"), 0).kind == MatchOutcome::Kind::MISS);
}

static void test_nonzero_key_pos() {
    auto r = chain_match(make_chain("0C", true), make_key("A00C"), 2);
    assert(r.kind == MatchOutcome::Kind::FOUND);
}

// Parent follows a light child at an odd position; the child chain starts at
// an odd nibble (phase 1) and its tail completes the key.
static void test_child_chain_odd_phase() {
    auto parent = make_chain("A5F0");
    parent.nodes.push_back(branch(1, {{0x7, 0, 99}}));
    std::string key = make_key("A73C");
    auto r = chain_match(parent, key, 0);
    assert(r.kind == MatchOutcome::Kind::FOLLOW_LIGHT && r.next_key_pos == 2 && r.light_child == 99);

    auto child = make_chain("3C", true, /*phase=*/0);  // starts at nibble 2 -> phase 0
    assert(chain_match(child, key, r.next_key_pos).kind == MatchOutcome::Kind::FOUND);

    // A light at split 0 makes the child start at nibble 1 (phase 1).
    auto p2 = make_chain("A5F0");
    p2.nodes.push_back(branch(0, {{0x4, 0, 77}}));
    std::string k2 = make_key("45F0");
    auto r2 = chain_match(p2, k2, 0);
    assert(r2.kind == MatchOutcome::Kind::FOLLOW_LIGHT && r2.next_key_pos == 1);
    auto c2 = make_chain("5F0", true, /*phase=*/1);
    assert(chain_match(c2, k2, r2.next_key_pos).kind == MatchOutcome::Kind::FOUND);
}

// chain_match_raw on encoded bytes must agree with chain_match on the decoded
// chain, for every chain of a real trie (varied key lengths -> both phases,
// nodes with and without record fields, many lights), many keys and start
// positions, both encodings.
static void test_match_raw_agrees_with_decoded() {
    const char* file = "/tmp/test_chain_match_raw.db";
    std::remove(file);
    auto dm_ptr = DiskManager::create(file); auto& dm = *dm_ptr;
    std::mt19937_64 rng(11);
    std::vector<std::string> keys;
    {
        // Tiny buckets: many chains, end nodes and buckets to compare.
        DiskTrie t(dm, "", TrieOptions{160, 4});
        std::uniform_int_distribution<int> len(2, 12), byte(0, 255);
        for (int i = 0; i < 3000; i++) {
            std::string k(len(rng), '\0');
            for (char& c : k) c = static_cast<char>(byte(rng) & 0x3F);  // small alphabet: shared prefixes
            t.insert(k, RecordPtr{static_cast<uint64_t>(i), 0});
            keys.push_back(k);  // duplicates are fine: they still match
        }
    }   // destroyed: hot cache flushed, so read_chain_at sees every chain

    std::vector<uint64_t> stack = {dm.root_block()};
    size_t chains = 0, checks = 0, follows = 0, buckets = 0, end_nodes = 0;
    std::uniform_int_distribution<size_t> pick(0, keys.size() - 1);
    while (!stack.empty()) {
        uint64_t addr = stack.back();
        stack.pop_back();
        ChainData c = dm.read_chain_at(addr);
        for (const auto& n : c.nodes)
            for (const auto& l : n.lights) stack.push_back(l.block);
        buckets   += c.bucket;
        end_nodes += c.has_end_node();

        uint8_t block[BLOCK_SIZE];
        assert(chain_encode(c, block));
        uint8_t slice[BLOCK_SIZE];
        size_t  slice_len = chain_encode_slice(c, slice, sizeof(slice));
        assert(slice_len > 0);

        for (int k = 0; k < 40; k++) {
            const std::string& key = keys[pick(rng)];
            for (size_t start : {size_t(0), size_t(rng() % (key.size() * 2 + 1))}) {
                MatchOutcome want = chain_match(c, key, start);
                for (int enc = 0; enc < 2; enc++) {
                    bool ok;
                    MatchOutcome got = enc ? chain_match_raw(slice, slice_len, key, start, ok)
                                           : chain_match_raw(block, BLOCK_SIZE, key, start, ok);
                    assert(ok);
                    assert(got.kind == want.kind);
                    if (want.kind == MatchOutcome::Kind::FOUND) {
                        assert(got.record.block_id == want.record.block_id);
                        assert(got.record.slot     == want.record.slot);
                    }
                    if (want.kind == MatchOutcome::Kind::FOLLOW_LIGHT) {
                        assert(got.next_key_pos == want.next_key_pos);
                        assert(got.node_index   == want.node_index);
                        assert(got.light_child  == want.light_child);
                        assert(got.light_rank   == want.light_rank);
                        assert(want.light_rank >= 0 &&
                               c.nodes[want.node_index].lights[static_cast<size_t>(want.light_rank)].block == want.light_child);
                        follows++;
                    }
                    checks++;
                }
            }
        }
        chains++;
    }
    assert(chains > 100 && checks > 10000 && follows > 100 && buckets > 50 && end_nodes > 10);
    std::remove(file);
}

static void test_match_raw_rejects_garbage() {
    uint8_t junk[64] = {1, 2, 3};
    bool ok = true;
    chain_match_raw(junk, sizeof(junk), "ab", 0, ok);
    assert(!ok);
    chain_match_raw(junk, 4, "ab", 0, ok);  // shorter than a header
    assert(!ok);
}

int main() {
    test_exact_match_found();
    test_exact_match_miss();
    test_follow_light();
    test_key_ends_at_node();
    test_key_shorter_than_chain_without_node();
    test_key_extends_past_chain();
    test_nonzero_key_pos();
    test_child_chain_odd_phase();
    test_match_raw_agrees_with_decoded();
    test_match_raw_rejects_garbage();
    return 0;
}
