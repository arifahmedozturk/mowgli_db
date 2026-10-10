#include "mql/engine.h"
#include <cassert>
#include <cstdio>

static const char* DATA_DIR = "/tmp/mql_test";

// QUERY appends "  [N chains]"; strip it to compare the row itself.
static std::string row(const std::string& result) {
    return result.substr(0, result.find("  ["));
}

static void setup()  { std::system("mkdir -p /tmp/mql_test"); }
static void cleanup() {
    std::system("rm -f /tmp/mql_test/*.trie /tmp/mql_test/*.heap /tmp/mql_test/*.schema /tmp/mql_test/wal.log");
}

static void test_create_and_query() {
    cleanup();
    Engine e(DATA_DIR);
    assert(e.exec("TABLE users(id string PRIMARY KEY, age number, name string)") == "OK");
    assert(e.exec("NEW(users, 'alice', 42, 'Alice Smith')") == "OK");
    assert(row(e.exec("QUERY(users, 'alice')")) == "alice | 42 | Alice Smith");
}

static void test_not_found() {
    cleanup();
    Engine e(DATA_DIR);
    e.exec("TABLE users(id string PRIMARY KEY, name string)");
    assert(e.exec("QUERY(users, 'nobody')") == "NOT FOUND");
}

static void test_duplicate_key() {
    cleanup();
    Engine e(DATA_DIR);
    e.exec("TABLE users(id string PRIMARY KEY, name string)");
    e.exec("NEW(users, 'alice', 'Alice')");
    assert(e.exec("NEW(users, 'alice', 'Alice2')") == "DUPLICATE KEY");
}

static void test_delete() {
    cleanup();
    Engine e(DATA_DIR);
    e.exec("TABLE users(id string PRIMARY KEY, name string)");
    e.exec("NEW(users, 'alice', 'Alice')");
    assert(e.exec("DELETE(users, 'alice')") == "OK");
    assert(e.exec("QUERY(users, 'alice')")  == "NOT FOUND");
    assert(e.exec("DELETE(users, 'alice')") == "NOT FOUND");
}

static void test_update() {
    cleanup();
    Engine e(DATA_DIR);
    e.exec("TABLE users(id string PRIMARY KEY, name string)");
    assert(e.exec("UPDATE(users, 'alice', 'Alice')") == "NOT FOUND");
    e.exec("NEW(users, 'alice', 'Alice')");
    assert(e.exec("UPDATE(users, 'alice', 'Alice2')") == "OK");
    assert(row(e.exec("QUERY(users, 'alice')")) == "alice | Alice2");
}

// Keys written before a restart must stay findable after the first insert
// post-restart (the Bloom filter is rebuilt on open, not persisted).
static void test_keys_survive_reopen_and_insert() {
    cleanup();
    {
        Engine e(DATA_DIR);
        e.exec("TABLE kv(k string PRIMARY KEY, v string)");
        for (int i = 0; i < 50; i++)
            e.exec("NEW(kv, 'k" + std::to_string(i) + "', 'v')");
    }
    Engine e(DATA_DIR);
    assert(e.exec("NEW(kv, 'new', 'v')") == "OK");
    for (int i = 0; i < 50; i++)
        assert(row(e.exec("QUERY(kv, 'k" + std::to_string(i) + "')")) == "k" + std::to_string(i) + " | v");
    assert(e.exec("QUERY(kv, 'missing')") == "NOT FOUND");
}

// Numbers must sort numerically: 256 > 255 even though its low byte is smaller.
static void test_number_range_order() {
    cleanup();
    Engine e(DATA_DIR);
    e.exec("TABLE nums(id number PRIMARY KEY, v string)");
    for (int n : {1000, 2, 256, 1, 255, 65536})
        e.exec("NEW(nums, " + std::to_string(n) + ", 'x')");
    assert(e.exec("RANGE(nums, 2, 300)") == "2 | x\n255 | x\n256 | x\n(3 rows)");
    assert(e.exec("RANGE(nums, 0, 70000)") ==
           "1 | x\n2 | x\n255 | x\n256 | x\n1000 | x\n65536 | x\n(6 rows)");
    assert(row(e.exec("QUERY(nums, 65536)")) == "65536 | x");
}

static void test_multiple_rows() {
    cleanup();
    Engine e(DATA_DIR);
    e.exec("TABLE products(id string PRIMARY KEY, price number)");
    for (int i = 0; i < 20; i++) {
        std::string cmd = "NEW(products, '" + std::to_string(i) + "', " + std::to_string(i * 10) + ")";
        assert(e.exec(cmd) == "OK");
    }
    for (int i = 0; i < 20; i++) {
        std::string expected = std::to_string(i) + " | " + std::to_string(i * 10);
        assert(row(e.exec("QUERY(products, '" + std::to_string(i) + "')")) == expected);
    }
}

static void test_number_pk() {
    cleanup();
    Engine e(DATA_DIR);
    e.exec("TABLE orders(id number PRIMARY KEY, item string)");
    e.exec("NEW(orders, 1, 'apple')");
    e.exec("NEW(orders, 2, 'banana')");
    assert(row(e.exec("QUERY(orders, 1)")) == "1 | apple");
    assert(row(e.exec("QUERY(orders, 2)")) == "2 | banana");
    assert(e.exec("QUERY(orders, 3)") == "NOT FOUND");
}

int main() {
    setup();
    test_create_and_query();
    test_not_found();
    test_duplicate_key();
    test_delete();
    test_update();
    test_number_range_order();
    test_keys_survive_reopen_and_insert();
    test_multiple_rows();
    test_number_pk();
    cleanup();
    return 0;
}
