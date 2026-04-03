#include "index/chain.h"
#include <cassert>

static RecordPtr SOME_RECORD{42, 0};

static ChainData make_chain(const char* bits, bool tail_has_record = false) {
    ChainData c;
    std::string b(bits);
    c.path_bit_len = static_cast<uint16_t>(b.size());
    c.tail_record  = tail_has_record ? SOME_RECORD : RecordPtr{};
    size_t bytes   = (b.size() + 7) / 8;
    c.path_bits.resize(bytes, 0);
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

static void test_exact_match_found() {
    auto c   = make_chain("10110010", true);
    auto key = make_key("10110010");
    auto r   = chain_match(c, key, 0);
    assert(r.kind == MatchOutcome::Kind::FOUND);
    assert(r.record.valid());
}

static void test_exact_match_miss() {
    auto c   = make_chain("10110010", true);
    auto key = make_key("10110011");
    auto r   = chain_match(c, key, 0);
    assert(r.kind == MatchOutcome::Kind::MISS);
}

static void test_follow_light() {
    auto c = make_chain("10110010");
    c.nodes.push_back({4, 2, 0, 99, RecordPtr{}}); // light child block = 99

    auto key = make_key("10111000"); // bit 4 = '1', chain bit 4 = '0' → light
    auto r   = chain_match(c, key, 0);
    assert(r.kind == MatchOutcome::Kind::FOLLOW_LIGHT);
    assert(r.node_index   == 0);
    assert(r.next_key_bit == 5);
}

static void test_terminal_mid_chain() {
    auto c = make_chain("10110010");
    c.nodes.push_back({3, 0, 0, NULL_BLOCK, RecordPtr{}}); // no record

    auto key = make_key("10110010");
    auto r   = chain_match(c, key, 0);
    assert(r.kind == MatchOutcome::Kind::MISS); // node has no record
}

static void test_key_extends_past_chain() {
    auto c   = make_chain("10110010", false); // tail has no record
    auto key = make_key("10110010");
    auto r   = chain_match(c, key, 0);
    assert(r.kind == MatchOutcome::Kind::MISS);
}

static void test_nonzero_key_bit_start() {
    auto c   = make_chain("1100", true);
    auto key = make_key("00001100");
    auto r   = chain_match(c, key, 4);
    assert(r.kind == MatchOutcome::Kind::FOUND);
}

int main() {
    test_exact_match_found();
    test_exact_match_miss();
    test_follow_light();
    test_terminal_mid_chain();
    test_key_extends_past_chain();
    test_nonzero_key_bit_start();
    return 0;
}
