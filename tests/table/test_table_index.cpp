// Secondary indexes (#96): a random mix of inserts, bulk inserts, updates and
// removes against a model, checking index scans (ranges, equality on a prefix
// of a composite index) after every round, across reopens, an index built over
// existing rows, and a dropped index.
#include "catalog/table.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <tuple>
#include <vector>

static const std::string DIR  = "/tmp/test_table_index";
static const std::string TRIE = DIR + "/users.trie";
static const std::string HEAP = DIR + "/users.heap";

static std::vector<uint8_t> num(uint64_t v) {
    std::vector<uint8_t> b(8);
    for (int j = 0; j < 8; j++) b[j] = static_cast<uint8_t>(v >> ((7 - j) * 8));
    return b;
}
static std::vector<uint8_t> str(const std::string& s) { return {s.begin(), s.end()}; }
static uint64_t as_num(const std::vector<uint8_t>& b) { uint64_t v = 0; for (uint8_t c : b) v = v << 8 | c; return v; }

struct Person { uint64_t age; std::string city; };
using Model = std::map<std::string, Person>;   // id -> columns

static Row row_of(const std::string& id, const Person& p) { return {str(id), num(p.age), str(p.city)}; }

// Rows the model says an index scan returns, in index order: (key cells, id).
template <class Key>
static std::vector<std::string> expect(const Model& m, Key key, bool (*in)(const Person&, const void*), const void* ctx) {
    std::vector<std::pair<decltype(key(Person{})), std::string>> v;
    for (const auto& [id, p] : m) if (in(p, ctx)) v.push_back({key(p), id});
    std::sort(v.begin(), v.end());
    std::vector<std::string> ids;
    for (auto& e : v) ids.push_back(e.second);
    return ids;
}

static std::vector<std::string> scan(Table& t, const std::string& ix, const Row& lo, const Row& hi) {
    std::vector<std::string> ids;
    t.index_range_each(ix, lo, hi, [&](Row&& r) { ids.emplace_back(r[0].begin(), r[0].end()); return true; });
    return ids;
}

static void check(Table& t, const Model& m, std::mt19937_64& rng, bool city_index) {
    assert(t.index_entry_count("by_age") == m.size());
    for (int q = 0; q < 10; q++) {
        uint64_t a = rng() % 60, b = rng() % 60;
        if (b < a) std::swap(a, b);
        struct R { uint64_t a, b; } r{a, b};
        auto want = expect(m, [](const Person& p) { return p.age; },
                           [](const Person& p, const void* c) { auto* r = static_cast<const R*>(c); return p.age >= r->a && p.age <= r->b; }, &r);
        assert(scan(t, "by_age", {num(a)}, {num(b)}) == want);
    }
    if (!city_index) return;
    assert(t.index_entry_count("by_city_age") == m.size());
    static const std::vector<std::string> cities = {"", "a", "ab", std::string("a\0b", 3), "b", std::string(1, '\0')};
    for (const auto& c : cities) {
        // Equality on the first column of (city, age), in age order.
        auto want = expect(m, [](const Person& p) { return std::make_tuple(p.city, p.age); },
                           [](const Person& p, const void* x) { return p.city == *static_cast<const std::string*>(x); }, &c);
        assert(scan(t, "by_city_age", {str(c)}, {str(c)}) == want);
        // A full (city, age) range within that city.
        uint64_t a = rng() % 60, b = a + rng() % 20;
        struct R { std::string c; uint64_t a, b; } r{c, a, b};
        auto want2 = expect(m, [](const Person& p) { return std::make_tuple(p.city, p.age); },
                            [](const Person& p, const void* x) { auto* r = static_cast<const R*>(x); return p.city == r->c && p.age >= r->a && p.age <= r->b; }, &r);
        assert(scan(t, "by_city_age", {str(c), num(a)}, {str(c), num(b)}) == want2);
    }
}

int main() {
    std::filesystem::remove_all(DIR);
    std::filesystem::create_directories(DIR);
    Schema schema{"users", {{"id", ColType::VARCHAR, 16}, {"age", ColType::UINT64, 8}, {"city", ColType::VARCHAR, 8}}, 0,
                  {{"by_age", {1}}}};
    std::mt19937_64 rng(96);
    static const std::vector<std::string> cities = {"", "a", "ab", std::string("a\0b", 3), "b", std::string(1, '\0')};
    auto person = [&] { return Person{rng() % 60, cities[rng() % cities.size()]}; };
    auto new_id = [&] { return "u" + std::to_string(rng() % 4000); };

    Model m;
    auto t = std::make_unique<Table>(Table::create(schema, TRIE, HEAP));
    bool city_index = false;
    for (int round = 0; round < 60; round++) {
        int op = static_cast<int>(rng() % 6);
        if (op <= 1) {
            for (int i = 0; i < 100; i++) {
                std::string id = new_id(); Person p = person();
                bool fresh = !m.count(id);
                assert(t->insert(row_of(id, p)) == fresh);
                if (fresh) m[id] = p;
            }
        } else if (op == 2) {
            std::vector<Row> rows;
            std::map<std::string, Person> batch;
            for (int i = 0; i < 200; i++) { std::string id = new_id(); Person p = person(); rows.push_back(row_of(id, p)); batch.emplace(id, p); }
            size_t want = 0;
            for (auto& [id, p] : batch) if (!m.count(id)) { want++; }
            // The first row given for a key wins (#35), which the batch map mirrors.
            std::map<std::string, Person> first;
            for (const auto& r : rows) first.emplace(std::string(r[0].begin(), r[0].end()), Person{as_num(r[1]), std::string(r[2].begin(), r[2].end())});
            assert(t->bulk_insert(rows) == want);
            for (auto& [id, p] : first) m.emplace(id, p);
        } else if (op == 3 && !m.empty()) {
            for (int i = 0; i < 50 && !m.empty(); i++) {   // updates that move rows between index keys
                auto it = m.begin(); std::advance(it, static_cast<long>(rng() % m.size()));
                Person p = person();
                assert(t->update(row_of(it->first, p)));
                it->second = p;
            }
        } else if (op == 4 && !m.empty()) {
            for (int i = 0; i < 60 && !m.empty(); i++) {
                auto it = m.begin(); std::advance(it, static_cast<long>(rng() % m.size()));
                assert(t->remove(str(it->first)));
                m.erase(it);
            }
        } else {
            schema = t->schema();
            t.reset();
            t = std::make_unique<Table>(Table::open(schema, TRIE, HEAP));
        }
        if (round == 20) {   // built over the rows already there
            t->create_index({"by_city_age", {2, 1}});
            city_index = true;
            assert(t->schema().find_index("by_city_age"));
        }
        check(*t, m, rng, city_index);
    }
    assert(m.size() > 300);

    t->drop_index("by_age");
    assert(!t->schema().find_index("by_age") && !std::filesystem::exists(DIR + "/users.by_age.idx"));
    schema = t->schema();
    t.reset();
    t = std::make_unique<Table>(Table::open(schema, TRIE, HEAP));
    assert(t->index_entry_count("by_city_age") == m.size());
    std::filesystem::remove_all(DIR);
    std::printf("test_table_index: %zu rows, all checks passed\n", m.size());
    return 0;
}
