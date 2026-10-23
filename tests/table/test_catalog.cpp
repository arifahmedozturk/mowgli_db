#include "catalog/codec.h"
#include "catalog/schema.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <functional>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

static std::vector<uint8_t> num(uint64_t v) {
    std::vector<uint8_t> b(8);
    for (int j = 0; j < 8; j++) b[j] = static_cast<uint8_t>(v >> ((7 - j) * 8));
    return b;
}

static bool throws(const std::function<void()>& f) {
    try { f(); } catch (const std::exception&) { return true; }
    return false;
}

static void test_schema_round_trip() {
    Schema s{"users", {{"id", ColType::VARCHAR, 64}, {"age", ColType::UINT64, 8}, {"name", ColType::VARCHAR, 32}}, 0,
             {{"by_age", {1}}, {"by_name_age", {2, 1}}}};
    Schema back = Schema::parse(s.to_text());
    assert(back.table_name == "users" && back.pk_col == 0 && back.cols.size() == 3);
    assert(back.indexes.size() == 2 && back.indexes[1].name == "by_name_age");
    assert((back.indexes[1].cols == std::vector<size_t>{2, 1}) && back.find_index("by_age") && !back.find_index("x"));
    for (size_t i = 0; i < 3; i++) {
        assert(back.cols[i].name == s.cols[i].name && back.cols[i].type == s.cols[i].type);
        assert(back.cols[i].max_size == s.cols[i].max_size);
    }
    const std::string path = "/tmp/test_catalog.schema";
    save_schema(path, s);
    assert(load_schema(path).to_text() == s.to_text());
    std::remove(path.c_str());

    assert(throws([] { Schema::parse("users\n0\n1\nid string 64\n"); }));             // no format line
    assert(throws([] { Schema::parse("mowgli-schema 1\nusers\n0\n2\nid string 64\n"); })); // truncated
    assert(throws([] { Schema::parse("mowgli-schema 1\nusers\n0\n1\nid float 8\n"); }));   // unknown type
    assert(throws([] { Schema::parse("mowgli-schema 1\nusers\n3\n1\nid string 64\n0\n"); }));  // bad pk
    assert(throws([] { Schema::parse("mowgli-schema 1\nusers\n0\n1\nid string 64\n1\nix 1 5\n"); }));  // bad index column
}

static void test_cell_text() {
    ColDef n{"n", ColType::UINT64, 8}, s{"s", ColType::VARCHAR, 4};
    assert(cell_from_text(n, "258") == num(258));
    assert(cell_to_text(ColType::UINT64, num(18446744073709551615ULL)) == "18446744073709551615");
    assert(throws([&] { cell_from_text(n, "12x"); }));
    assert(throws([&] { cell_from_text(n, "18446744073709551616"); }));   // 2^64
    assert(cell_to_text(ColType::VARCHAR, cell_from_text(s, "abcdef")) == "abcdef");   // bounds may exceed max_size
}

// Composite keys sort exactly like their tuples and decode back to them.
static void test_composite_key_order() {
    std::mt19937_64 rng(95);
    const std::vector<ColType> types = {ColType::VARCHAR, ColType::UINT64, ColType::VARCHAR};
    const uint8_t alphabet[] = {0x00, 0x01, 0x7F, 0xFE, 0xFF, 'a', 'b'};
    auto rand_str = [&] {
        std::vector<uint8_t> v(rng() % 5);
        for (auto& c : v) c = alphabet[rng() % sizeof(alphabet)];
        return v;
    };
    using Tuple = std::tuple<std::vector<uint8_t>, uint64_t, std::vector<uint8_t>>;
    std::vector<Tuple> tuples;
    for (int i = 0; i < 3000; i++) tuples.emplace_back(rand_str(), rng() % 4, rand_str());

    std::vector<std::pair<std::string, size_t>> keys;
    for (size_t i = 0; i < tuples.size(); i++) {
        const auto& [a, b, c] = tuples[i];
        std::string k = encode_key(types, {a, num(b), c});
        auto back = decode_key(types, k);
        assert(back[0] == a && back[1] == num(b) && back[2] == c);
        keys.emplace_back(std::move(k), i);
    }
    for (size_t i = 0; i < keys.size(); i++) {
        for (size_t j = 0; j < 40; j++) {
            const auto& x = keys[i];
            const auto& y = keys[rng() % keys.size()];
            const bool tuple_less = tuples[x.second] < tuples[y.second];
            const bool tuple_eq   = tuples[x.second] == tuples[y.second];
            assert((x.first < y.first) == tuple_less);
            assert((x.first == y.first) == tuple_eq);
        }
    }
}

static void test_decode_rejects_malformed() {
    const std::vector<ColType> sn = {ColType::VARCHAR, ColType::UINT64};
    assert(throws([&] { decode_key(sn, std::string("ab", 2)); }));                   // no terminator
    assert(throws([&] { decode_key(sn, std::string("a\0\x01", 3)); }));              // bad escape
    assert(throws([&] { decode_key(sn, std::string("a\0\0" "1234567", 10)); }));     // short number
    assert(throws([&] { decode_key({ColType::UINT64}, std::string(9, 'x')); }));     // trailing byte
}

int main() {
    test_schema_round_trip();
    test_cell_text();
    test_composite_key_order();
    test_decode_rejects_malformed();
    std::printf("test_catalog: all checks passed\n");
    return 0;
}
