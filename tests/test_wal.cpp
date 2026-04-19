#include "mql/engine.h"
#include "storage/wal.h"
#include <cassert>
#include <filesystem>
#include <fstream>

static const std::string DIR = "/tmp/wal_test";

static void setup() {
    std::filesystem::remove_all(DIR);
    std::filesystem::create_directories(DIR);
}

// --- WAL unit tests ---

static void test_wal_commit_leaves_nothing() {
    std::string path = DIR + "/a.wal";
    std::filesystem::remove(path);
    {
        Wal w = Wal::open(path);
        uint64_t t = w.begin("NEW(x, 1)");
        w.commit(t);
    }
    Wal w = Wal::open(path);
    assert(w.recover().empty());
}

static void test_wal_uncommitted_is_recovered() {
    std::string path = DIR + "/b.wal";
    std::filesystem::remove(path);
    {
        Wal w = Wal::open(path);
        w.begin("NEW(x, 42)"); // no commit
    }
    Wal w = Wal::open(path);
    auto pending = w.recover();
    assert(pending.size() == 1);
    assert(pending[0] == "NEW(x, 42)");
}

static void test_wal_order_preserved() {
    std::string path = DIR + "/c.wal";
    std::filesystem::remove(path);
    {
        Wal w = Wal::open(path);
        w.begin("CMD_A"); // uncommitted
        uint64_t t = w.begin("CMD_B");
        w.commit(t);      // committed
        w.begin("CMD_C"); // uncommitted
    }
    Wal w = Wal::open(path);
    auto pending = w.recover();
    assert(pending.size() == 2);
    // txn_ids are monotone so CMD_A (lower id) comes before CMD_C
    assert(pending[0] == "CMD_A");
    assert(pending[1] == "CMD_C");
}

static void test_wal_truncate() {
    std::string path = DIR + "/d.wal";
    std::filesystem::remove(path);
    {
        Wal w = Wal::open(path);
        w.begin("CMD_X"); // uncommitted
        w.truncate();
    }
    Wal w = Wal::open(path);
    assert(w.recover().empty());
}

// --- Engine crash-recovery integration test ---

static void test_engine_recovery() {
    std::filesystem::remove_all(DIR + "/eng");
    std::filesystem::create_directories(DIR + "/eng");

    // First engine: create table, insert a row, then "crash" before the second
    // insert's commit by manually appending an uncommitted CMD record to the WAL.
    {
        Engine e(DIR + "/eng");
        e.exec("TABLE kv(k string PRIMARY KEY, v string)");
        e.exec("NEW(kv, 'a', 'alpha')");
    }

    // Simulate a crash: write a CMD record to the WAL with no COMMIT.
    // When the next Engine opens it should replay this and insert 'b'.
    {
        Wal w = Wal::open(DIR + "/eng/wal.log");
        w.begin("NEW(kv, 'b', 'beta')"); // no commit
        // w goes out of scope — fd closed, record on disk
    }

    // Second engine: should recover 'b' from the WAL.
    {
        Engine e(DIR + "/eng");
        // 'a' was committed normally
        assert(e.exec("QUERY(kv, 'a')").find("alpha") != std::string::npos);
        // 'b' was recovered from the uncommitted WAL record
        assert(e.exec("QUERY(kv, 'b')").find("beta") != std::string::npos);
    }
}

int main() {
    setup();
    test_wal_commit_leaves_nothing();
    test_wal_uncommitted_is_recovered();
    test_wal_order_preserved();
    test_wal_truncate();
    test_engine_recovery();
    return 0;
}
