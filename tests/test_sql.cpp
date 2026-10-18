// The SQL-like front end (#103, #92): what the parser builds from each
// statement and rejects, how a WHERE clause is planned, and how commands are
// routed in a cluster.
#include "mql/ast.h"
#include "mql/engine.h"
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>

static bool rejected(const std::string& text) {
    try { parse(text); } catch (const std::runtime_error&) { return true; }
    return false;
}

static std::vector<uint8_t> be(uint64_t v) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; i++) b[i] = uint8_t(v >> (56 - 8 * i));
    return b;
}

static void test_parse() {
    auto c = std::get<CreateTable>(parse("create table t (id NUMBER primary key, name varchar(20), n int);"));
    assert(c.table == "t" && c.columns.size() == 3);
    assert(c.columns[0].primary_key && c.columns[0].type == "NUMBER");
    assert(c.columns[1].type == "VARCHAR" && c.columns[1].size == 20 && !c.columns[1].primary_key);

    auto ins = std::get<Insert>(parse("INSERT INTO t VALUES (1, 'a''b'), (2, '')"));
    assert(ins.rows.size() == 2 && ins.rows[0][1].text == "a'b" && ins.rows[1][1].text.empty());
    assert(ins.rows[0][0].kind == Literal::Kind::NUMBER && ins.rows[0][1].kind == Literal::Kind::STRING);

    auto s = std::get<Select>(parse("SELECT a, b FROM t WHERE a >= 3 AND b BETWEEN 'x' AND 'y' AND c < 9 LIMIT 5"));
    assert(s.columns.size() == 2 && !s.count && s.limit == 5u && s.where.size() == 3);
    assert(s.where[0].op == Condition::Op::GE && s.where[0].column == "a" && s.where[0].value.text == "3");
    assert(s.where[1].op == Condition::Op::BETWEEN && s.where[1].value.text == "x" && s.where[1].upper.text == "y");
    assert(s.where[2].op == Condition::Op::LT);
    assert(std::get<Select>(parse("SELECT COUNT(*) FROM t")).count);
    auto in = std::get<Select>(parse("SELECT * FROM t WHERE a IN (1, 'x', 3)")).where;
    assert(in.size() == 1 && in[0].op == Condition::Op::IN && in[0].list.size() == 3 && in[0].list[1].text == "x");
    assert(std::get<Select>(parse("SELECT * FROM t")).columns.empty());

    auto u = std::get<Update>(parse("UPDATE t SET a = 1, b = 'z' WHERE id = 4"));
    assert(u.set.size() == 2 && u.set[1].first == "b" && u.set[1].second.text == "z" && u.where.size() == 1);
    auto d = std::get<Delete>(parse("DELETE FROM t"));
    assert(d.table == "t" && d.where.empty());

    assert(std::holds_alternative<ShowTables>(parse("SHOW TABLES")));
    assert(std::get<Chains>(parse("SHOW CHAINS t")).table == "t");
    assert(std::get<Describe>(parse("DESCRIBE t")).table == "t");
    assert(std::get<DropTable>(parse("DROP TABLE t")).table == "t");
    auto ci = std::get<CreateIndex>(parse("CREATE INDEX by_ab ON t (a, b)"));
    assert(ci.index == "by_ab" && ci.table == "t" && ci.columns.size() == 2 && ci.columns[1] == "b");
    auto di = std::get<DropIndex>(parse("drop index by_ab on t"));
    assert(di.index == "by_ab" && di.table == "t");
    assert(is_write(parse("CREATE INDEX i ON t (a)")) && is_write(parse("DROP INDEX i ON t")));
    assert(std::holds_alternative<Stats>(parse("stats")));
    // A keyword as a name keeps its spelling.
    assert(std::get<Select>(parse("SELECT Key FROM t")).columns[0] == "Key");

    assert(is_write(parse("INSERT INTO t VALUES (1)")) && is_write(parse("DROP TABLE t")));
    assert(!is_write(parse("SELECT * FROM t")) && !is_write(parse("COMPACT t")));

    for (const char* bad : {"", "SELECT", "SELECT * t", "SELECT * FROM t WHERE", "SELECT * FROM t WHERE a",
                            "SELECT * FROM t WHERE a = ", "SELECT * FROM t LIMIT x", "INSERT INTO t VALUES ()",
                            "INSERT INTO t (1)", "UPDATE t a = 1", "CREATE TABLE t ()", "CREATE TABLE t (a)",
                            "DELETE t", "SELECT * FROM t; SELECT * FROM t", "SELECT * FROM t WHERE a = 1 OR b = 2",
                            "FROB t", "'x'", "SELECT * FROM t WHERE a IN ()", "SELECT * FROM t WHERE a IN 1",
                            "CREATE INDEX i ON t ()", "CREATE INDEX i t (a)", "CREATE INDEX ON t (a)", "DROP INDEX i"})
        assert(rejected(bad));
}

static void test_plan() {
    Schema s{"t", {{"id", ColType::UINT64, 8}, {"v", ColType::VARCHAR, 8}}, 0};
    auto where = [](const std::string& w) { return std::get<Select>(parse("SELECT * FROM t WHERE " + w)).where; };
    using K = Engine::Plan::Kind;
    Engine::Plan p = Engine::plan(s, where("id = 7"));
    assert(p.kind == K::KEY && p.key == be(7));
    p = Engine::plan(s, where("id BETWEEN 3 AND 9 AND id < 8"));
    assert(p.kind == K::RANGE && p.lo == be(3) && p.hi == be(8));
    p = Engine::plan(s, where("id > 3"));
    assert(p.kind == K::RANGE && p.lo == be(3) && p.hi > be(UINT64_MAX));
    p = Engine::plan(s, where("id <= 3 AND v = 'x'"));
    assert(p.kind == K::RANGE && p.lo.empty() && p.hi == be(3));
    assert(Engine::plan(s, where("v = 'x'")).kind == K::SCAN);
    assert(Engine::plan(s, {}).kind == K::SCAN);
    assert(Engine::plan(s, where("id = 1 AND id = 2")).kind == K::NONE);
    assert(Engine::plan(s, where("id > 5 AND id < 2")).kind == K::NONE);
    assert(Engine::plan(s, where("id = 9 AND id < 5")).kind == K::NONE);
    assert(Engine::plan(s, where("id = 4 AND id BETWEEN 1 AND 6")).kind == K::KEY);
    p = Engine::plan(s, where("id IN (9, 3, 9, 5)"));
    assert(p.kind == K::KEYS && p.keys.size() == 3 && p.keys[0] == be(3) && p.keys[2] == be(9));
    p = Engine::plan(s, where("id IN (9, 3, 5) AND id < 6"));
    assert(p.kind == K::KEYS && p.keys.size() == 2);
    p = Engine::plan(s, where("id IN (9, 3) AND id = 3"));
    assert(p.kind == K::KEY && p.key == be(3));
    assert(Engine::plan(s, where("id IN (9, 3) AND id = 4")).kind == K::NONE);
    assert(Engine::plan(s, where("v IN ('a', 'b')")).kind == K::SCAN);
    // exact: the plan's keys decide the match without reading the rows.
    assert(Engine::plan(s, where("id = 3")).exact);
    assert(Engine::plan(s, where("id IN (3, 4) AND id <= 3")).exact);
    assert(!Engine::plan(s, where("id IN (3, 4) AND id < 4")).exact);
    assert(!Engine::plan(s, where("id = 3 AND v = 'x'")).exact);
}

static void test_route() {
    const std::string dir = "/tmp/test_sql";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    Engine e(dir);
    e.exec("CREATE TABLE t (id NUMBER PRIMARY KEY, v TEXT)");
    using K = Engine::Route::Kind;
    Engine::Route r = e.route("INSERT INTO t VALUES (5, 'x')");
    assert(r.write && r.kind == K::KEY && r.key == std::string("\0\0\0\0\0\0\0\5", 8));
    r = e.route("INSERT INTO t VALUES (5, 'x'), (6, 'y')");
    assert(r.write && r.kind == K::ANY);
    r = e.route("SELECT * FROM t WHERE id = 5");
    assert(!r.write && r.kind == K::KEY);
    r = e.route("SELECT v FROM t WHERE id BETWEEN 1 AND 9");
    assert(r.kind == K::RANGE && r.lo == std::string("\0\0\0\0\0\0\0\1", 8) && r.hi == std::string("\0\0\0\0\0\0\0\x09", 8));
    // Row counts and limits are not merged across nodes, so they run where they arrive.
    assert(e.route("SELECT COUNT(*) FROM t WHERE id BETWEEN 1 AND 9").kind == K::ANY);
    assert(e.route("SELECT * FROM t WHERE id BETWEEN 1 AND 9 LIMIT 2").kind == K::ANY);
    r = e.route("UPDATE t SET v = 'z' WHERE id = 5");
    assert(r.write && r.kind == K::KEY);
    r = e.route("DELETE FROM t WHERE id > 3");
    assert(r.write && r.kind == K::ANY);
    assert(e.route("SELECT * FROM nosuch WHERE id = 1").kind == K::ANY);
    assert(e.route("CREATE TABLE u (k NUMBER PRIMARY KEY)").write);
    bool threw = false;
    try { e.route("SELEC"); } catch (const std::runtime_error&) { threw = true; }
    assert(threw);
    std::filesystem::remove_all(dir);
}

int main() {
    test_parse();
    test_plan();
    test_route();
    std::puts("test_sql: all passed");
    return 0;
}
