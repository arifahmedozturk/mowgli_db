// The engine running SQL-like statements (#103): tables, rows, WHERE clauses
// on the key and on other columns, partial UPDATE, range DELETE, COUNT,
// LIMIT, projection, DROP / SHOW / DESCRIBE.
#include "mql/engine.h"
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>

static const std::string DATA_DIR = "/tmp/mql_test";

static void fresh() {
    std::filesystem::remove_all(DATA_DIR);
    std::filesystem::create_directories(DATA_DIR);
}

static bool throws(Engine& e, const std::string& cmd) {
    try { e.exec(cmd); } catch (const std::runtime_error&) { return true; }
    return false;
}

static void test_create_insert_select() {
    fresh();
    Engine e(DATA_DIR);
    assert(e.exec("CREATE TABLE users (id VARCHAR(16) PRIMARY KEY, age NUMBER, name TEXT)") == "OK");
    assert(e.exec("INSERT INTO users VALUES ('alice', 42, 'Alice Smith')") == "INSERT 1");
    assert(e.exec("select * from users where id = 'alice';") == "alice | 42 | Alice Smith\n(1 rows)");
    assert(e.exec("SELECT name, age FROM users WHERE id = 'alice'") == "Alice Smith | 42\n(1 rows)");
    assert(e.exec("SELECT * FROM users WHERE id = 'nobody'") == "(0 rows)");
    // A stored key is skipped, not overwritten.
    assert(e.exec("INSERT INTO users VALUES ('alice', 1, 'Other')") == "INSERT 0");
    assert(e.exec("SELECT age FROM users WHERE id = 'alice'") == "42\n(1 rows)");
    // Several rows at once go in as a bulk load; repeats and stored keys are skipped.
    assert(e.exec("INSERT INTO users VALUES ('bob', 30, 'B'), ('carol', 25, 'C'), ('bob', 1, 'X'), ('alice', 2, 'Y')")
           == "INSERT 2");
    assert(e.exec("SELECT COUNT(*) FROM users") == "3");
}

static void test_where_on_key() {
    fresh();
    Engine e(DATA_DIR);
    e.exec("CREATE TABLE nums (id NUMBER PRIMARY KEY, v VARCHAR(8))");
    for (int n : {1000, 2, 256, 1, 255, 65536})
        e.exec("INSERT INTO nums VALUES (" + std::to_string(n) + ", 'x')");
    // Numbers sort numerically: 256 > 255 even though its low byte is smaller.
    assert(e.exec("SELECT * FROM nums WHERE id BETWEEN 2 AND 300") == "2 | x\n255 | x\n256 | x\n(3 rows)");
    assert(e.exec("SELECT id FROM nums WHERE id > 2 AND id < 1000") == "255\n256\n(2 rows)");
    assert(e.exec("SELECT id FROM nums WHERE id >= 256") == "256\n1000\n65536\n(3 rows)");
    assert(e.exec("SELECT id FROM nums WHERE id <= 2") == "1\n2\n(2 rows)");
    assert(e.exec("SELECT id FROM nums") == "1\n2\n255\n256\n1000\n65536\n(6 rows)");
    assert(e.exec("SELECT id FROM nums LIMIT 2") == "1\n2\n(2 rows)");
    assert(e.exec("SELECT id FROM nums WHERE id > 100 LIMIT 1") == "255\n(1 rows)");
    assert(e.exec("SELECT id FROM nums LIMIT 0") == "(0 rows)");
    assert(e.exec("SELECT COUNT(*) FROM nums WHERE id BETWEEN 2 AND 300") == "3");
    // IN on the key: those keys, in key order, repeats and missing ones dropped.
    assert(e.exec("SELECT id FROM nums WHERE id IN (256, 1, 7, 256, 65536)") == "1\n256\n65536\n(3 rows)");
    assert(e.exec("SELECT id FROM nums WHERE id IN (256, 1, 65536) AND id > 200") == "256\n65536\n(2 rows)");
    assert(e.exec("SELECT id FROM nums WHERE id IN (1, 2) AND id IN (2, 255)") == "2\n(1 rows)");
    assert(e.exec("SELECT id FROM nums WHERE id IN (1, 2) LIMIT 1") == "1\n(1 rows)");
    // An exclusive bound is checked on each key, not taken as inclusive.
    assert(e.exec("SELECT id FROM nums WHERE id IN (1, 2) AND id < 2") == "1\n(1 rows)");
    assert(e.exec("SELECT id FROM nums WHERE id = 2 AND id > 2") == "(0 rows)");
    assert(e.exec("DELETE FROM nums WHERE id IN (1, 2) AND id > 1") == "DELETE 1");
    e.exec("INSERT INTO nums VALUES (2, 'x')");
    assert(e.exec("DELETE FROM nums WHERE id IN (1, 2, 3)") == "DELETE 2");
    // Contradictions match nothing.
    assert(e.exec("SELECT id FROM nums WHERE id = 2 AND id = 3") == "(0 rows)");
    assert(e.exec("SELECT id FROM nums WHERE id > 300 AND id < 200") == "(0 rows)");
    assert(e.exec("SELECT id FROM nums WHERE id = 2 AND id > 5") == "(0 rows)");
}

static void test_where_on_other_columns() {
    fresh();
    Engine e(DATA_DIR);
    e.exec("CREATE TABLE p (id NUMBER PRIMARY KEY, city TEXT, age NUMBER)");
    e.exec("INSERT INTO p VALUES (1, 'oslo', 30), (2, 'rome', 41), (3, 'oslo', 52), (4, 'lima', 30)");
    assert(e.exec("SELECT id FROM p WHERE city = 'oslo'") == "1\n3\n(2 rows)");
    assert(e.exec("SELECT id FROM p WHERE age >= 40 AND city = 'oslo'") == "3\n(1 rows)");
    assert(e.exec("SELECT id FROM p WHERE id >= 2 AND age = 30") == "4\n(1 rows)");
    // A key lookup still checks the other conditions.
    assert(e.exec("SELECT id FROM p WHERE id = 1 AND age = 30") == "1\n(1 rows)");
    assert(e.exec("SELECT id FROM p WHERE id = 1 AND age = 31") == "(0 rows)");
    assert(e.exec("SELECT COUNT(*) FROM p WHERE age = 30") == "2");
    assert(e.exec("SELECT id FROM p WHERE city IN ('rome', 'lima', 'paris')") == "2\n4\n(2 rows)");
    assert(throws(e, "SELECT id FROM p WHERE height = 3"));   // unknown column
    assert(throws(e, "SELECT id FROM p WHERE age = 'old'"));  // wrong kind of value
}

static void test_update() {
    fresh();
    Engine e(DATA_DIR);
    e.exec("CREATE TABLE u (id VARCHAR(8) PRIMARY KEY, name TEXT, score NUMBER)");
    assert(e.exec("UPDATE u SET name = 'x' WHERE id = 'alice'") == "UPDATE 0");
    e.exec("INSERT INTO u VALUES ('alice', 'Alice', 1), ('bob', 'Bob', 2), ('cat', 'Cat', 3)");
    // Only the named columns change.
    assert(e.exec("UPDATE u SET score = 10 WHERE id = 'alice'") == "UPDATE 1");
    assert(e.exec("SELECT * FROM u WHERE id = 'alice'") == "alice | Alice | 10\n(1 rows)");
    assert(e.exec("UPDATE u SET name = 'Z', score = 0 WHERE score BETWEEN 2 AND 3") == "UPDATE 2");
    assert(e.exec("SELECT * FROM u") == "alice | Alice | 10\nbob | Z | 0\ncat | Z | 0\n(3 rows)");
    assert(throws(e, "UPDATE u SET id = 'b' WHERE id = 'alice'"));   // the key cannot change
    // Every other column set, on given keys: the row is replaced without being
    // read, and only stored keys count.
    assert(e.exec("UPDATE u SET name = 'N', score = 7 WHERE id IN ('alice', 'zed')") == "UPDATE 1");
    assert(e.exec("SELECT * FROM u WHERE id = 'alice'") == "alice | N | 7\n(1 rows)");
    assert(e.exec("SELECT COUNT(*) FROM u") == "3");
    // One column named twice is not every column: the other keeps its value.
    assert(e.exec("UPDATE u SET name = 'a', name = 'b' WHERE id = 'alice'") == "UPDATE 1");
    assert(e.exec("SELECT * FROM u WHERE id = 'alice'") == "alice | b | 7\n(1 rows)");
    // A condition on another column still reads the row.
    assert(e.exec("UPDATE u SET name = 'Q', score = 1 WHERE id = 'alice' AND score = 99") == "UPDATE 0");
}

static void test_delete() {
    fresh();
    Engine e(DATA_DIR);
    e.exec("CREATE TABLE d (id NUMBER PRIMARY KEY, v TEXT)");
    for (int i = 0; i < 10; i++) e.exec("INSERT INTO d VALUES (" + std::to_string(i) + ", 'v')");
    assert(e.exec("DELETE FROM d WHERE id = 3") == "DELETE 1");
    assert(e.exec("DELETE FROM d WHERE id = 3") == "DELETE 0");
    assert(e.exec("DELETE FROM d WHERE id BETWEEN 5 AND 7") == "DELETE 3");
    assert(e.exec("SELECT id FROM d") == "0\n1\n2\n4\n8\n9\n(6 rows)");
    assert(e.exec("DELETE FROM d") == "DELETE 6");
    assert(e.exec("SELECT COUNT(*) FROM d") == "0");
}

static void test_schema_statements() {
    fresh();
    Engine e(DATA_DIR);
    e.exec("CREATE TABLE b (k NUMBER PRIMARY KEY)");
    e.exec("CREATE TABLE a (id VARCHAR(10) PRIMARY KEY, n INT)");
    assert(e.exec("SHOW TABLES") == "a\nb\n(2 tables)");
    assert(e.exec("DESCRIBE a") == "id VARCHAR(10) PRIMARY KEY\nn NUMBER");
    assert(throws(e, "CREATE TABLE a (x NUMBER PRIMARY KEY)"));               // exists
    assert(throws(e, "CREATE TABLE c (x NUMBER)"));                           // no key
    assert(throws(e, "CREATE TABLE c (x NUMBER PRIMARY KEY, y NUMBER PRIMARY KEY)"));
    assert(throws(e, "CREATE TABLE c (x NUMBER PRIMARY KEY, x TEXT)"));       // duplicate column
    assert(throws(e, "CREATE TABLE c (x NUMBER PRIMARY KEY, y BLOB)"));       // unknown type
    assert(throws(e, "CREATE TABLE c (x VARCHAR(5000) PRIMARY KEY)"));        // too wide
    // A string longer than its column is rejected.
    assert(throws(e, "INSERT INTO a VALUES ('much too long a key', 1)"));
    e.exec("INSERT INTO a VALUES ('x', 1)");
    assert(e.exec("DROP TABLE a") == "OK");
    assert(e.exec("SHOW TABLES") == "b\n(1 tables)");
    assert(throws(e, "SELECT * FROM a"));
    for (const auto& f : std::filesystem::directory_iterator(DATA_DIR))
        assert(f.path().filename().string().rfind("a.", 0) != 0);   // its files are gone
    // The name can be used again, starting empty.
    e.exec("CREATE TABLE a (id NUMBER PRIMARY KEY)");
    assert(e.exec("SELECT COUNT(*) FROM a") == "0");
}

static void test_literals_and_names() {
    fresh();
    Engine e(DATA_DIR);
    // A column may be named like a keyword.
    e.exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)");
    e.exec("INSERT INTO kv VALUES ('it''s', 'a ''quoted'' value')");
    assert(e.exec("SELECT value FROM kv WHERE key = 'it''s'") == "a 'quoted' value\n(1 rows)");
    assert(throws(e, "SELECT * FROM kv WHERE key = 'unterminated"));
    assert(throws(e, "SELEKT * FROM kv"));
    assert(throws(e, "SELECT * FROM kv extra"));
    assert(e.exec("STATS").rfind("inflight: ", 0) == 0);
    assert(e.exec("HELP").find("SELECT") != std::string::npos);
}

// Rows written before a restart are found after it, including after a write
// (the Bloom filter is loaded or rebuilt on open).
static void test_reopen() {
    fresh();
    {
        Engine e(DATA_DIR);
        e.exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)");
        for (int i = 0; i < 50; i++) e.exec("INSERT INTO kv VALUES ('k" + std::to_string(i) + "', 'v')");
    }
    Engine e(DATA_DIR);
    assert(e.exec("INSERT INTO kv VALUES ('new', 'v')") == "INSERT 1");
    for (int i = 0; i < 50; i++)
        assert(e.exec("SELECT v FROM kv WHERE k = 'k" + std::to_string(i) + "'") == "v\n(1 rows)");
    assert(e.exec("SELECT COUNT(*) FROM kv") == "51");
}

int main() {
    test_create_insert_select();
    test_where_on_key();
    test_where_on_other_columns();
    test_update();
    test_delete();
    test_schema_statements();
    test_literals_and_names();
    test_reopen();
    std::filesystem::remove_all(DATA_DIR);
    std::puts("test_engine: all passed");
    return 0;
}
