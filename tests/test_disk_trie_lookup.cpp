#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include "index/chain.h"
#include <cassert>
#include <cstdio>

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

int main() {
    test_empty();
    test_single_chain_found();
    test_single_chain_miss();
    test_two_chains();
    cleanup();
    return 0;
}
