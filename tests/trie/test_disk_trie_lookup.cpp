#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include "index/chain.h"
#include <cassert>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

static const char* TEST_FILE = "/tmp/test_disk_trie.db";
static void cleanup() { std::remove(TEST_FILE); }

static RecordPtr REC{42, 0};

static uint8_t hexval(char c) { return static_cast<uint8_t>(c <= '9' ? c - '0' : c - 'A' + 10); }

// Chain whose path is the given hex digits (one nibble each).
static ChainData make_chain(const char* hex, bool tail_has_record = false) {
    ChainData c;
    for (const char* p = hex; *p; p++) c.path.push_back(hexval(*p));
    c.path_len    = static_cast<uint16_t>(c.path.size());
    c.tail_record = tail_has_record ? REC : RecordPtr{};
    return c;
}

// Key bytes from an even number of hex digits.
static std::string make_key(const char* hex) {
    std::string h(hex), key;
    for (size_t i = 0; i + 1 < h.size(); i += 2)
        key.push_back(static_cast<char>(hexval(h[i]) << 4 | hexval(h[i + 1])));
    return key;
}

static void test_single_chain_found() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;

    auto chain = make_chain("B2", true);
    dm.set_root_block(dm.write_chain(chain));

    DiskTrie t(dm);
    size_t    chains = 0;
    RecordPtr ptr;
    assert(t.lookup(make_key("B2"), &ptr, &chains));
    assert(chains == 1);
    assert(ptr.block_id == REC.block_id);
}

static void test_single_chain_miss() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    dm.set_root_block(dm.write_chain(make_chain("B2", true)));
    DiskTrie t(dm);
    assert(!t.lookup(make_key("B3")));
}

static void test_two_chains() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;

    auto light = make_chain("C3", true);   // starts at nibble 2
    uint64_t light_block = dm.write_chain(light);

    auto root_chain = make_chain("B2A0", true);
    // Branch at nibble 1 (chain digit 2); digit F goes to light_block.
    ChainNode n;
    n.split  = 1;
    n.lights = {{0xF, 0, light_block}};
    root_chain.nodes.push_back(n);
    dm.set_root_block(dm.write_chain(root_chain));

    DiskTrie t(dm);
    size_t chains = 0;

    assert(t.lookup(make_key("B2A0"), nullptr, &chains));
    assert(chains == 1);

    chains = 0;
    assert(t.lookup(make_key("BFC3"), nullptr, &chains));
    assert(chains == 2);
    assert(!t.lookup(make_key("BEC3")));   // no light child for digit E

    assert(!t.lookup(make_key("00")));
}

static void test_empty() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    assert(!t.lookup(make_key("B2")));
}

// The Bloom filter grows in stages with the key count (#22) and keeps each
// key's bits in one block (#76): no false negatives across stages, a low
// false-positive rate, and the stages survive a save and reload.
static void test_bloom_grows_with_keys() {
    cleanup();
    const std::string bloom = std::string(TEST_FILE) + ".bloom";
    std::remove(bloom.c_str());
    std::mt19937_64 rng(22);
    auto key = [](uint64_t v) { std::string k(8, '\0'); for (int j = 0; j < 8; j++) k[j] = char(v >> (56 - 8 * j)); return k; };
    std::vector<std::string> keys;
    for (int i = 0; i < 700000; i++) keys.push_back(key(rng()));
    size_t stages = 0;
    {
        auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
        DiskTrie t(dm, bloom);
        for (size_t i = 0; i < keys.size(); i++) t.insert(keys[i], RecordPtr{i + 1, 0});
        dm.set_key_count(keys.size());
        stages = t.bloom_stages().size();
        assert(stages == 2);   // 256K keys, then a 1M stage
        uint64_t total = 0;
        for (const auto& st : t.bloom_stages()) total += st.count;
        assert(total == keys.size());
        for (const auto& k : keys) assert(t.lookup(k));
        size_t fp = 0;
        const size_t probes = 200000;
        for (size_t i = 0; i < probes; i++) fp += t.bloom_may_contain_for_test(key(rng()));
        assert(fp < probes * 3 / 100);   // two stages at ~1% each
    }
    auto dm_ptr = DiskManager::open(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm, bloom);
    assert(t.bloom_loaded() && t.bloom_stages().size() == stages);
    for (size_t i = 0; i < keys.size(); i += 97) assert(t.lookup(keys[i]));
    std::remove(bloom.c_str());
}

int main() {
    test_empty();
    test_single_chain_found();
    test_single_chain_miss();
    test_two_chains();
    cleanup();
    test_bloom_grows_with_keys();
    return 0;
}
