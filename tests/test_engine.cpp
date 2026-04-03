#include "mql/engine.h"
#include <cassert>
#include <cstdio>

static const char* DATA_DIR = "/tmp/mql_test";

static void setup()  { std::system("mkdir -p /tmp/mql_test"); }
static void cleanup() {
    std::system("rm -f /tmp/mql_test/*.trie /tmp/mql_test/*.heap");
}

static void test_create_and_query() {
    cleanup();
    Engine e(DATA_DIR);
    assert(e.exec("TABLE users(id string PRIMARY KEY, age number, name string)") == "OK");
    assert(e.exec("UPDATE(users, 'alice', 42, 'Alice Smith')") == "OK");
    assert(e.exec("QUERY(users, 'alice')") == "alice | 42 | Alice Smith");
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
    e.exec("UPDATE(users, 'alice', 'Alice')");
    assert(e.exec("UPDATE(users, 'alice', 'Alice2')") == "DUPLICATE KEY");
}

static void test_delete() {
    cleanup();
    Engine e(DATA_DIR);
    e.exec("TABLE users(id string PRIMARY KEY, name string)");
    e.exec("UPDATE(users, 'alice', 'Alice')");
    assert(e.exec("DELETE(users, 'alice')") == "OK");
    assert(e.exec("QUERY(users, 'alice')")  == "NOT FOUND");
    assert(e.exec("DELETE(users, 'alice')") == "NOT FOUND");
}

static void test_multiple_rows() {
    cleanup();
    Engine e(DATA_DIR);
    e.exec("TABLE products(id string PRIMARY KEY, price number)");
    for (int i = 0; i < 20; i++) {
        std::string cmd = "UPDATE(products, '" + std::to_string(i) + "', " + std::to_string(i * 10) + ")";
        assert(e.exec(cmd) == "OK");
    }
    for (int i = 0; i < 20; i++) {
        std::string expected = std::to_string(i) + " | " + std::to_string(i * 10);
        assert(e.exec("QUERY(products, '" + std::to_string(i) + "')") == expected);
    }
}

static void test_number_pk() {
    cleanup();
    Engine e(DATA_DIR);
    e.exec("TABLE orders(id number PRIMARY KEY, item string)");
    e.exec("UPDATE(orders, 1, 'apple')");
    e.exec("UPDATE(orders, 2, 'banana')");
    assert(e.exec("QUERY(orders, 1)") == "1 | apple");
    assert(e.exec("QUERY(orders, 2)") == "2 | banana");
    assert(e.exec("QUERY(orders, 3)") == "NOT FOUND");
}

int main() {
    setup();
    test_create_and_query();
    test_not_found();
    test_duplicate_key();
    test_delete();
    test_multiple_rows();
    test_number_pk();
    cleanup();
    return 0;
}
