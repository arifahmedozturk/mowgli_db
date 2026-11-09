// Crash safety of committed writes (#28): a child process runs random
// mutations through the engine and reports each command once exec() has
// returned (its WAL commit is on disk); the parent kills it with SIGKILL at a
// random moment, reopens the data directory (which replays any uncommitted
// command) and checks that the table holds exactly the committed commands'
// effect, with or without the one that was in flight.
#include "index/disk_trie.h"
#include "mql/engine.h"
#include "catalog/schema.h"
#include "storage/disk_manager.h"
#include "storage/wal.h"
#include <functional>
#include <cassert>
#include <csignal>
#include <cstdio>
#include <filesystem>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

static const std::string DIR = "/tmp/test_crash";
using Model = std::map<uint64_t, uint64_t>;

// The i-th command of a run, and its effect on the model.
struct Op { std::string cmd; std::vector<std::pair<uint64_t, uint64_t>> rows; char kind; };

static Op make_op(std::mt19937_64& rng) {
    auto k = [&] { return rng() % 3000; };
    auto v = [&] { return rng() % 1000000; };
    Op op;
    switch (rng() % 10) {
    case 0: case 1: case 2: case 3: {
        uint64_t a = k(), b = v();
        op = Op{"INSERT INTO t VALUES (" + std::to_string(a) + ", " + std::to_string(b) + ")", {{a, b}}, 'N'};
        break;
    }
    case 4: case 5: {
        uint64_t a = k(), b = v();
        op = Op{"UPDATE t SET v = " + std::to_string(b) + " WHERE id = " + std::to_string(a), {{a, b}}, 'U'};
        break;
    }
    case 6: case 7: {
        uint64_t a = k();
        op = Op{"DELETE FROM t WHERE id = " + std::to_string(a), {{a, 0}}, 'D'};
        break;
    }
    default: {
        op = Op{"INSERT INTO t VALUES", {}, 'B'};
        for (int i = 0; i < 20; i++) {
            uint64_t a = k(), b = v();
            op.cmd += std::string(i ? ", (" : " (") + std::to_string(a) + ", " + std::to_string(b) + ")";
            op.rows.push_back({a, b});
        }
    }
    }
    return op;
}

static void apply_op(Model& m, const Op& op) {
    for (const auto& [a, b] : op.rows) {
        if (op.kind == 'N' || op.kind == 'B') m.emplace(a, b);   // insert if absent; first row wins
        else if (op.kind == 'U') { auto it = m.find(a); if (it != m.end()) it->second = b; }
        else m.erase(a);
    }
}

static Model read_table(Engine& e) {
    Model m;
    std::istringstream rows(e.exec("SELECT * FROM t"));
    std::string line;
    while (std::getline(rows, line)) {
        uint64_t a, b;
        if (std::sscanf(line.c_str(), "%lu | %lu", &a, &b) == 2) m[a] = b;
    }
    return m;
}

static void one_crash(uint64_t seed) {
    std::filesystem::remove_all(DIR);
    std::filesystem::create_directories(DIR);
    { Engine e(DIR); e.exec("CREATE TABLE t (id NUMBER PRIMARY KEY, v NUMBER)"); }

    int fds[2];
    assert(::pipe(fds) == 0);
    pid_t pid = ::fork();
    if (pid == 0) {
        ::close(fds[0]);
        Engine e(DIR);
        std::mt19937_64 rng(seed);
        for (uint32_t i = 0;; i++) {
            e.exec(make_op(rng).cmd);
            if (::write(fds[1], &i, sizeof(i)) != sizeof(i)) ::_exit(1);   // committed
        }
    }
    ::close(fds[1]);
    std::mt19937_64 when(seed * 31 + 7);
    const uint32_t target = 50 + when() % 600;
    uint32_t acked = 0, i;
    while (acked < target && ::read(fds[0], &i, sizeof(i)) == sizeof(i)) acked = i + 1;
    ::usleep(static_cast<useconds_t>(when() % 3000));   // land the kill anywhere in a command
    ::kill(pid, SIGKILL);
    ::waitpid(pid, nullptr, 0);
    while (::read(fds[0], &i, sizeof(i)) == sizeof(i)) acked = i + 1;
    ::close(fds[0]);

    Model committed, with_next;
    std::mt19937_64 rng(seed);
    for (uint32_t n = 0; n < acked; n++) apply_op(committed, make_op(rng));
    with_next = committed;
    apply_op(with_next, make_op(rng));

    Engine e(DIR);   // replays an uncommitted command, if one was logged
    Model got = read_table(e);
    if (got != committed && got != with_next) {
        std::fprintf(stderr, "seed %lu: %u committed commands, table has %zu rows, expected %zu (or %zu)\n",
                     seed, acked, got.size(), committed.size(), with_next.size());
        assert(false);
    }
}

// A crash in the middle of uncommitted work leaves the last committed
// structure whole: slots freed by the work (merges, pruning, moved buckets)
// stay intact until commit, so nothing on disk points at a freed slot. Tiny
// buckets make the removes merge and prune a lot.
static void crash_before_commit() {
    const std::string file = DIR + "/trie.db";
    std::filesystem::remove_all(DIR);
    std::filesystem::create_directories(DIR);
    auto key = [](uint64_t i) { return "k" + std::to_string(100000 + i); };
    constexpr uint64_t N = 6000;
    pid_t pid = ::fork();
    if (pid == 0) {
        auto dm = DiskManager::create(file);
        DiskTrie t(*dm, "", TrieOptions{128, 16});
        t.set_durable(true);
        for (uint64_t i = 0; i < N; i++) t.insert(key(i), RecordPtr{i + 1, 0});
        t.commit();
        for (uint64_t i = 0; i < N; i++) if (i % 4) t.remove(key(i));   // uncommitted
        ::_exit(0);   // no commit, no destructors
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    auto dm = DiskManager::open(file);
    DiskTrie t(*dm, "", TrieOptions{128, 16});
    for (uint64_t i = 0; i < N; i += 4) {   // untouched by the uncommitted removes
        RecordPtr p;
        assert(t.lookup(key(i), &p) && p.block_id == i + 1);
    }
    std::vector<std::pair<std::string, RecordPtr>> all;
    t.range_scan(std::string(), std::string(8, '\xFF'), all);   // must not reach a freed slot
    assert(all.size() >= N / 4);
}

// Secondary indexes after a crash mid-statement (#123). The heap is written
// straight into the mapped file, but index chains wait in the cache until
// commit, so a crash can leave the heap ahead of the index. A child does a
// statement's table work and logs the command without committing it, then
// dies; reopening replays the command, and every row must still be found
// through the index. Many rows moving to one index value split its buckets,
// so the index work does not all land in place.
static constexpr uint64_t IX_ROWS = 4000;

static std::string num(uint64_t n) { return std::to_string(n); }

static void indexed_table() {
    std::filesystem::remove_all(DIR);
    std::filesystem::create_directories(DIR);
    Engine e(DIR);
    e.exec("CREATE TABLE t (id NUMBER PRIMARY KEY, v NUMBER)");
    e.exec("CREATE INDEX by_v ON t (v)");
    for (uint64_t lo = 0; lo < IX_ROWS; lo += 500) {
        std::string cmd = "INSERT INTO t VALUES";
        for (uint64_t id = lo; id < lo + 500; id++)
            cmd += std::string(id == lo ? " (" : ", (") + num(id) + ", " + num(id * 1000 + 1) + ")";
        e.exec(cmd);
    }
}

// In a child: log cmd as begun, run work on the table, die before commit.
static void crash_after(const std::string& cmd, const std::function<void(Table&)>& work) {
    pid_t pid = ::fork();
    if (pid == 0) {
        Wal w = Wal::open(DIR + "/wal.log");
        w.begin(cmd);
        Table t = Table::open(load_schema(DIR + "/t.schema"), DIR + "/t.trie", DIR + "/t.heap");
        t.set_durable(true);
        work(t);
        ::_exit(0);   // no commit, no destructors
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

static Row num_row(const Schema& s, uint64_t id, uint64_t v) {
    return {Engine::encode({Literal::Kind::NUMBER, num(id)}, s.cols[0]),
            Engine::encode({Literal::Kind::NUMBER, num(v)},  s.cols[1])};
}

static void crash_mid_indexed_update() {
    indexed_table();
    crash_after("UPDATE t SET v = 7 WHERE id < " + num(IX_ROWS), [](Table& t) {
        for (uint64_t id = 0; id < IX_ROWS; id++) t.update(num_row(t.schema(), id, 7));
    });
    Engine e(DIR);
    std::string got = e.exec("SELECT id FROM t WHERE v = 7");
    std::string want = "(" + num(IX_ROWS) + " rows)";
    if (got.size() < want.size() || got.compare(got.size() - want.size(), want.size(), want) != 0) {
        std::fprintf(stderr, "indexed update: index finds %s, want %s\n",
                     got.substr(got.rfind('(')).c_str(), want.c_str());
        assert(false);
    }
}

static void crash_mid_indexed_insert() {
    indexed_table();
    std::string cmd = "INSERT INTO t VALUES";
    for (uint64_t id = IX_ROWS; id < 2 * IX_ROWS; id++)
        cmd += std::string(id == IX_ROWS ? " (" : ", (") + num(id) + ", 9)";
    crash_after(cmd, [](Table& t) {
        for (uint64_t id = IX_ROWS; id < 2 * IX_ROWS; id++) t.insert(num_row(t.schema(), id, 9));
    });
    Engine e(DIR);
    std::string got = e.exec("SELECT id FROM t WHERE v = 9");
    std::string want = "(" + num(IX_ROWS) + " rows)";
    if (got.size() < want.size() || got.compare(got.size() - want.size(), want.size(), want) != 0) {
        std::fprintf(stderr, "indexed insert: index finds %s, want %s\n",
                     got.substr(got.rfind('(')).c_str(), want.c_str());
        assert(false);
    }
}

int main() {
    crash_mid_indexed_update();
    crash_mid_indexed_insert();
    crash_before_commit();
    for (uint64_t seed = 1; seed <= 12; seed++) one_crash(seed);
    std::filesystem::remove_all(DIR);
    std::printf("test_crash: indexed update and insert crashes, uncommitted crash and 12 kills, committed state survived each\n");
    return 0;
}
