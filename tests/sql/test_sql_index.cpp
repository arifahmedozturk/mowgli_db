// Secondary indexes through the query language (#97): CREATE / DROP INDEX,
// the planner choosing an index, and results checked against a model across
// writes, reopens and an index drop.
#include "mql/engine.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <map>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

static const std::string DIR = "/tmp/test_sql_index";

static void fresh() {
    std::filesystem::remove_all(DIR);
    std::filesystem::create_directories(DIR);
}

// A SELECT's row lines, sorted (an index returns rows in index order).
static std::vector<std::string> lines(const std::string& out) {
    std::vector<std::string> v;
    std::istringstream in(out);
    std::string l;
    while (std::getline(in, l)) if (l.empty() || l[0] != '(') v.push_back(l);
    std::sort(v.begin(), v.end());
    return v;
}

static void test_plan_choice() {
    Schema s{"t", {{"id", ColType::UINT64, 8}, {"city", ColType::VARCHAR, 8}, {"age", ColType::UINT64, 8}}, 0};
    s.indexes = {{"by_city", {1}}, {"by_city_age", {1, 2}}, {"by_age", {2}}};
    auto where = [](const std::string& w) { return std::get<Select>(parse("SELECT * FROM t WHERE " + w)).where; };
    using K = Engine::Plan::Kind;
    Engine::Plan p = Engine::plan(s, where("city = 'oslo'"));
    assert(p.kind == K::INDEX && p.index_ranges.size() == 1 && p.index_ranges[0].first.size() == 1);
    // The longest usable prefix wins: = on city, then bounds on age.
    p = Engine::plan(s, where("city = 'oslo' AND age > 30"));
    assert(p.kind == K::INDEX && p.index == "by_city_age" && p.index_ranges[0].first.size() == 2);
    p = Engine::plan(s, where("STARTS_WITH(city, 'os')"));
    assert(p.kind == K::INDEX && p.index_ranges.size() == 1);
    // The scan covers just the prefix: its upper bound starts with it too.
    assert(p.index_ranges[0].first[0] == (std::vector<uint8_t>{'o', 's'}));
    assert(p.index_ranges[0].second[0].size() > 2 && p.index_ranges[0].second[0][0] == 'o' && p.index_ranges[0].second[0][1] == 's');
    p = Engine::plan(s, where("age BETWEEN 3 AND 9"));
    assert(p.kind == K::INDEX && p.index == "by_age");
    // IN on an indexed column: one scan per value.
    p = Engine::plan(s, where("city IN ('a', 'b', 'c')"));
    assert(p.kind == K::INDEX && p.index_ranges.size() == 3);
    // The key comes first; columns not leading an index are filters on a scan.
    assert(Engine::plan(s, where("id = 3 AND city = 'oslo'")).kind == K::KEY);
    assert(Engine::plan(s, where("id > 3 AND city = 'oslo'")).kind == K::RANGE);
    Schema plain = s;
    plain.indexes.clear();
    assert(Engine::plan(plain, where("city = 'oslo'")).kind == K::SCAN);
}

static void test_statements() {
    fresh();
    Engine e(DIR);
    e.exec("CREATE TABLE p (id NUMBER PRIMARY KEY, city VARCHAR(8), age NUMBER)");
    e.exec("INSERT INTO p VALUES (1, 'oslo', 30), (2, 'rome', 41), (3, 'oslo', 52), (4, 'lima', 30)");
    assert(e.exec("CREATE INDEX by_city ON p (city)") == "OK");   // built over the existing rows
    assert(e.exec("DESCRIBE p") == "id NUMBER PRIMARY KEY\ncity VARCHAR(8)\nage NUMBER\nINDEX by_city (city)");
    assert(e.exec("SELECT id FROM p WHERE city = 'oslo'") == "1\n3\n(2 rows)");
    assert(e.exec("SELECT id FROM p WHERE city = 'oslo' AND age > 40") == "3\n(1 rows)");
    assert(e.exec("SELECT id FROM p WHERE city = 'oslo' LIMIT 1") == "1\n(1 rows)");
    assert(e.exec("SELECT COUNT(*) FROM p WHERE city IN ('rome', 'lima')") == "2");
    assert(e.exec("SELECT COUNT(*) FROM p WHERE city IN ('rome', 'rome')") == "1");   // each value once
    assert(lines(e.exec("SELECT id FROM p WHERE city IN ('rome', 'lima') LIMIT 1")).size() == 1);
    // Writes keep the index current.
    e.exec("UPDATE p SET city = 'rome' WHERE id = 1");
    e.exec("DELETE FROM p WHERE id = 3");
    e.exec("INSERT INTO p VALUES (5, 'oslo', 19)");
    assert(e.exec("SELECT id FROM p WHERE city = 'oslo'") == "5\n(1 rows)");
    assert(lines(e.exec("SELECT id FROM p WHERE city = 'rome'")) == (std::vector<std::string>{"1", "2"}));
    assert(e.exec("UPDATE p SET age = 1 WHERE city = 'rome'") == "UPDATE 2");
    assert(e.exec("DELETE FROM p WHERE city = 'rome' AND age = 1") == "DELETE 2");
    assert(e.exec("SELECT COUNT(*) FROM p") == "2");

    auto throws = [&](const std::string& cmd) {
        try { e.exec(cmd); } catch (const std::runtime_error&) { return true; }
        return false;
    };
    assert(throws("CREATE INDEX by_city ON p (age)"));        // name taken
    assert(throws("CREATE INDEX x ON p (height)"));           // no such column
    assert(throws("CREATE INDEX x ON p (age, age)"));
    assert(throws("CREATE INDEX x ON nosuch (age)"));
    assert(throws("DROP INDEX nosuch ON p"));
    assert(e.exec("DROP INDEX by_city ON p") == "OK");
    assert(e.exec("SELECT id FROM p WHERE city = 'oslo'") == "5\n(1 rows)");   // now a scan
    assert(e.exec("DESCRIBE p") == "id NUMBER PRIMARY KEY\ncity VARCHAR(8)\nage NUMBER");
}

// The index survives a restart, and a CREATE INDEX left unfinished by a
// crash is replayed from the WAL.
static void test_persistence() {
    fresh();
    {
        Engine e(DIR);
        e.exec("CREATE TABLE p (id NUMBER PRIMARY KEY, city TEXT)");
        e.exec("INSERT INTO p VALUES (1, 'oslo'), (2, 'rome')");
        e.exec("CREATE INDEX by_city ON p (city)");
    }
    {
        Engine e(DIR);
        assert(e.exec("DESCRIBE p").find("INDEX by_city (city)") != std::string::npos);
        e.exec("INSERT INTO p VALUES (3, 'oslo')");
        assert(e.exec("SELECT id FROM p WHERE city = 'oslo'") == "1\n3\n(2 rows)");
        e.exec("DROP INDEX by_city ON p");
    }
    {
        Wal w = Wal::open(DIR + "/wal.log");
        w.begin("CREATE INDEX by_city2 ON p (city)");   // logged, never committed
    }
    {
        Engine e(DIR);
        assert(e.exec("DESCRIBE p").find("INDEX by_city2 (city)") != std::string::npos);
        assert(e.exec("SELECT id FROM p WHERE city = 'rome'") == "2\n(1 rows)");
        e.exec("DROP TABLE p");
    }
    for (const auto& f : std::filesystem::directory_iterator(DIR))
        assert(f.path().filename().string().rfind("p.", 0) != 0);   // index files went too
}

// Random rows, writes and queries; every query's rows are checked against a
// model, with and without the indexes.
static void test_against_model() {
    fresh();
    std::mt19937_64 rng(17);
    Engine e(DIR);
    e.exec("CREATE TABLE r (id NUMBER PRIMARY KEY, a NUMBER, b VARCHAR(4))");
    e.exec("CREATE INDEX by_a ON r (a)");
    e.exec("CREATE INDEX by_b_a ON r (b, a)");
    struct Cols { uint64_t a; std::string b; };
    std::map<uint64_t, Cols> m;
    auto rb = [&] { std::string s(1 + rng() % 3, 'p'); for (char& c : s) c = char('p' + rng() % 3); return s; };
    auto row_text = [](uint64_t id, const Cols& c) { return std::to_string(id) + " | " + std::to_string(c.a) + " | " + c.b; };
    size_t queries = 0;
    for (int round = 0; round < 300; round++) {
        uint64_t id = rng() % 400;
        switch (rng() % 4) {
        case 0: { Cols c{rng() % 20, rb()};
                  e.exec("INSERT INTO r VALUES (" + std::to_string(id) + ", " + std::to_string(c.a) + ", '" + c.b + "')");
                  m.emplace(id, c); break; }
        case 1: { uint64_t a = rng() % 20;
                  e.exec("UPDATE r SET a = " + std::to_string(a) + " WHERE id = " + std::to_string(id));
                  if (m.count(id)) m[id].a = a; break; }
        case 2: e.exec("DELETE FROM r WHERE id = " + std::to_string(id)); m.erase(id); break;
        default: break;
        }
        if (round == 150) e.exec("DROP INDEX by_a ON r");
        // Queries the indexes can serve, and the rows the model says match.
        uint64_t a = rng() % 20, a2 = a + rng() % 6;
        std::string b = rb();
        std::vector<std::pair<std::string, std::function<bool(const Cols&)>>> qs = {
            {"a = " + std::to_string(a),                       [&](const Cols& c) { return c.a == a; }},
            {"a BETWEEN " + std::to_string(a) + " AND " + std::to_string(a2), [&](const Cols& c) { return c.a >= a && c.a <= a2; }},
            {"b = '" + b + "'",                                [&](const Cols& c) { return c.b == b; }},
            {"b = '" + b + "' AND a < " + std::to_string(a),   [&](const Cols& c) { return c.b == b && c.a < a; }},
            {"b IN ('p', '" + b + "') AND a >= " + std::to_string(a), [&](const Cols& c) { return (c.b == "p" || c.b == b) && c.a >= a; }},
            {"STARTS_WITH(b, '" + b.substr(0, 1) + "')", [&](const Cols& c) { return c.b.compare(0, 1, b, 0, 1) == 0; }},
            {"STARTS_WITH(b, '" + b + "') AND a > " + std::to_string(a), [&](const Cols& c) { return c.b.rfind(b, 0) == 0 && c.a > a; }},
            // A range on the index's first column: its second is only a filter.
            {"b <= '" + b + "' AND a = " + std::to_string(a), [&](const Cols& c) { return c.b <= b && c.a == a; }},
        };
        for (const auto& [w, pred] : qs) {
            std::vector<std::string> want;
            for (const auto& [k, c] : m) if (pred(c)) want.push_back(row_text(k, c));
            std::sort(want.begin(), want.end());
            assert(lines(e.exec("SELECT * FROM r WHERE " + w)) == want);
            queries++;
        }
    }
    assert(queries == 2400);
}

int main() {
    test_plan_choice();
    test_statements();
    test_persistence();
    test_against_model();
    std::filesystem::remove_all(DIR);
    std::puts("test_sql_index: all passed");
    return 0;
}
