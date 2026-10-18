// Crash safety of committed writes (#28): a child process runs random
// mutations through the engine and reports each command once exec() has
// returned (its WAL commit is on disk); the parent kills it with SIGKILL at a
// random moment, reopens the data directory (which replays any uncommitted
// command) and checks that the table holds exactly the committed commands'
// effect, with or without the one that was in flight.
#include "index/disk_trie.h"
#include "mql/engine.h"
#include "storage/disk_manager.h"
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
        op = Op{"NEW(t, " + std::to_string(a) + ", " + std::to_string(b) + ")", {{a, b}}, 'N'};
        break;
    }
    case 4: case 5: {
        uint64_t a = k(), b = v();
        op = Op{"UPDATE(t, " + std::to_string(a) + ", " + std::to_string(b) + ")", {{a, b}}, 'U'};
        break;
    }
    case 6: case 7: {
        uint64_t a = k();
        op = Op{"DELETE(t, " + std::to_string(a) + ")", {{a, 0}}, 'D'};
        break;
    }
    default: {
        op = Op{"BULK t", {}, 'B'};
        for (int i = 0; i < 20; i++) {
            uint64_t a = k(), b = v();
            op.cmd += " (" + std::to_string(a) + ", " + std::to_string(b) + ")";
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
    std::istringstream rows(e.exec("RANGE(t, 0, 18446744073709551615)"));
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
    { Engine e(DIR); e.exec("TABLE t(id number PRIMARY KEY, v number)"); }

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

int main() {
    crash_before_commit();
    for (uint64_t seed = 1; seed <= 12; seed++) one_crash(seed);
    std::filesystem::remove_all(DIR);
    std::printf("test_crash: uncommitted crash and 12 kills, committed state survived each\n");
    return 0;
}
