#include "mql/engine.h"
#include "storage/wal.h"
#include <atomic>
#include <cassert>
#include <filesystem>
#include <cstdio>
#include <fstream>
#include <thread>
#include <vector>

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

// Concurrent commits share fdatasyncs (#21): with 8 writers, far fewer syncs
// than commits; and nothing is left pending.
static void test_wal_group_commit() {
    std::string path = DIR + "/e.wal";
    std::filesystem::remove(path);
    constexpr int THREADS = 8, EACH = 200;
    {
        Wal w = Wal::open(path);
        std::vector<std::thread> ts;
        for (int t = 0; t < THREADS; t++)
            ts.emplace_back([&w, t] {
                for (int i = 0; i < EACH; i++) w.commit(w.begin("CMD_" + std::to_string(t) + "_" + std::to_string(i)));
            });
        for (auto& th : ts) th.join();
        assert(w.syncs() >= 1);
        assert(w.syncs() < THREADS * EACH / 2);
    }
    Wal w = Wal::open(path);
    assert(w.recover().empty());
}

// A single writer syncs once per commit, and Sync::NONE never syncs.
static void test_wal_sync_modes() {
    std::string path = DIR + "/f.wal";
    std::filesystem::remove(path);
    {
        Wal w = Wal::open(path);
        for (int i = 0; i < 10; i++) w.commit(w.begin("CMD"));
        assert(w.syncs() == 10);
    }
    std::filesystem::remove(path);
    {
        Wal w = Wal::open(path, Wal::Sync::NONE);
        for (int i = 0; i < 10; i++) w.commit(w.begin("CMD"));
        w.begin("OPEN");
        assert(w.syncs() == 0);
    }
    Wal w = Wal::open(path);
    auto pending = w.recover();   // unsynced records still reach the file
    assert(pending.size() == 1 && pending[0] == "OPEN");
}

// The log is emptied once it passes truncate_at with nothing in flight, but
// never while a command is open, whose record must survive.
static void test_wal_bounded() {
    std::string path = DIR + "/g.wal";
    std::filesystem::remove(path);
    {
        Wal w = Wal::open(path, Wal::Sync::NONE, 4096);
        for (int i = 0; i < 2000; i++) w.commit(w.begin("NEW(t, " + std::to_string(i) + ")"));
        assert(w.file_bytes() <= 4096);
        assert(std::filesystem::file_size(path) == w.file_bytes());
        w.begin("STILL_OPEN");
        for (int i = 0; i < 2000; i++) w.commit(w.begin("NEW(t, " + std::to_string(i) + ")"));
        assert(w.file_bytes() > 4096);   // held: STILL_OPEN is in flight
    }
    Wal w = Wal::open(path);
    auto pending = w.recover();
    assert(pending.size() == 1 && pending[0] == "STILL_OPEN");
}

// --- Engine crash-recovery integration test ---

static void test_engine_recovery() {
    std::filesystem::remove_all(DIR + "/eng");
    std::filesystem::create_directories(DIR + "/eng");

    // First engine: create table, insert a row, then "crash" before the second
    // insert's commit by manually appending an uncommitted CMD record to the WAL.
    {
        Engine e(DIR + "/eng");
        e.exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)");
        e.exec("INSERT INTO kv VALUES ('a', 'alpha')");
    }

    // Simulate a crash: write a CMD record to the WAL with no COMMIT.
    // When the next Engine opens it should replay this and insert 'b'.
    {
        Wal w = Wal::open(DIR + "/eng/wal.log");
        w.begin("INSERT INTO kv VALUES ('b', 'beta')"); // no commit
        // w goes out of scope — fd closed, record on disk
    }

    // Second engine: should recover 'b' from the WAL.
    {
        Engine e(DIR + "/eng");
        // 'a' was committed normally
        assert(e.exec("SELECT * FROM kv WHERE k = 'a'").find("alpha") != std::string::npos);
        // 'b' was recovered from the uncommitted WAL record
        assert(e.exec("SELECT * FROM kv WHERE k = 'b'").find("beta") != std::string::npos);
    }
}

// A mutation that fails still closes its log record, so it is not replayed
// on the next start (and does not hold the log open).
static void test_engine_failed_mutation_closed() {
    std::filesystem::remove_all(DIR + "/fail");
    std::filesystem::create_directories(DIR + "/fail");
    {
        Engine e(DIR + "/fail");
        e.exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)");
        bool threw = false;
        try { e.exec("INSERT INTO nosuch VALUES ('a', 'alpha')"); } catch (const std::exception&) { threw = true; }
        assert(threw);
    }
    Wal w = Wal::open(DIR + "/fail/wal.log");
    assert(w.recover().empty());
}

int main() {
    setup();
    test_wal_commit_leaves_nothing();
    test_wal_uncommitted_is_recovered();
    test_wal_order_preserved();
    test_wal_truncate();
    test_wal_group_commit();
    test_wal_sync_modes();
    test_wal_bounded();
    test_engine_recovery();
    test_engine_failed_mutation_closed();
    std::puts("test_wal: all passed");
    return 0;
}
