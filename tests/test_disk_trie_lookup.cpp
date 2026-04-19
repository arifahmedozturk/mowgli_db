#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include "index/chain.h"
#include <cassert>
#include <cstdio>

static const char* TEST_FILE = "/tmp/test_disk_trie.db";
static void cleanup() { std::remove(TEST_FILE); }

static RecordPtr REC{42, 0};

static ChainData make_chain(const char* bits, bool tail_has_record = false) {
    ChainData c;
    std::string b(bits);
    c.path_bit_len = static_cast<uint16_t>(b.size());
    c.tail_record  = tail_has_record ? REC : RecordPtr{};
    c.path_bits.resize((b.size() + 7) / 8, 0);
    for (size_t i = 0; i < b.size(); i++)
        if (b[i] == '1')
            c.path_bits[i / 8] |= 1u << (7 - i % 8);
    return c;
}

static std::string make_key(const char* bits) {
    std::string b(bits);
    while (b.size() % 8) b += '0';
    std::string key(b.size() / 8, '\0');
    for (size_t i = 0; i < b.size(); i++)
        if (b[i] == '1')
            key[i / 8] |= 1u << (7 - i % 8);
    return key;
}

static void test_single_chain_found() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;

    auto chain = make_chain("10110010", true);
    dm.set_root_block(dm.write_chain(chain));

    DiskTrie t(dm);
    size_t    chains = 0;
    RecordPtr ptr;
    assert(t.lookup(make_key("10110010"), &ptr, &chains));
    assert(chains == 1);
    assert(ptr.block_id == REC.block_id);
}

static void test_single_chain_miss() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    dm.set_root_block(dm.write_chain(make_chain("10110010", true)));
    DiskTrie t(dm);
    assert(!t.lookup(make_key("10110011")));
}

static void test_two_chains() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;

    auto light = make_chain("111", true);
    uint64_t light_block = dm.write_chain(light);

    auto root_chain = make_chain("10110010", true);
    // Branch at bit 4 (chain bit = '0'), light child = light_block.
    root_chain.nodes.push_back({4, 0, 0, light_block, RecordPtr{}});
    dm.set_root_block(dm.write_chain(root_chain));

    DiskTrie t(dm);
    size_t chains = 0;

    assert(t.lookup(make_key("10110010"), nullptr, &chains));
    assert(chains == 1);

    chains = 0;
    assert(t.lookup(make_key("10111111"), nullptr, &chains));
    assert(chains == 2);

    assert(!t.lookup(make_key("00000000")));
}

static void test_empty() {
    cleanup();
    auto dm_ptr = DiskManager::create(TEST_FILE); auto& dm = *dm_ptr;
    DiskTrie t(dm);
    assert(!t.lookup(make_key("10110010")));
}

int main() {
    test_empty();
    test_single_chain_found();
    test_single_chain_miss();
    test_two_chains();
    cleanup();
    return 0;
}
