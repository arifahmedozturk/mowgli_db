// WAL on vs off: the same MQL mutations through Engine, with write-ahead
// logging that waits for the disk (one fdatasync per mutation, shared by
// concurrent writers, #21), logging that does not (survives a process crash
// only), and no logging.
//
// Usage: bench_wal [N] [data_dir] [--csv file]

#include "bench/metrics.h"
#include "mql/engine.h"
#include <chrono>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <string>

using Clock = std::chrono::steady_clock;
using Dur   = std::chrono::duration<double>;

static std::string key(size_t i) { return "'k" + std::to_string(i) + "'"; }

// Runs op(i) for i in [0, n), prints and records throughput and latency as <name>_<mode>.
static void run_phase(const std::string& name, const std::string& mode, size_t n,
                      const std::function<void(size_t)>& op, Metrics& m) {
    Latencies lat;
    auto t0 = Clock::now();
    for (size_t i = 0; i < n; i++) {
        auto ti = Clock::now();
        op(i);
        lat.record(Dur(Clock::now() - ti).count());
    }
    double secs = Dur(Clock::now() - t0).count();

    std::cout << "  " << std::left << std::setw(8) << name << std::right
              << std::setw(12) << n / secs / 1000 << " K ops/sec   p50/p99/p999 "
              << lat.p_us(0.50) << " / " << lat.p_us(0.99) << " / " << lat.p_us(0.999) << " us\n";

    std::string metric = name + "_" + mode;
    m.add(metric, n / secs, "ops/s");
    m.add_latency(metric, lat);
}

static void run_mode(bool wal, Wal::Sync sync, size_t n, const std::string& data_dir, Metrics& m) {
    std::string mode = !wal ? "wal_off" : sync == Wal::Sync::COMMIT ? "wal_on" : "wal_nosync";
    std::string dir  = data_dir + "/" + mode;
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    std::cout << "WAL " << mode << "\n" << std::fixed << std::setprecision(2);
    Engine e(dir, wal, sync);
    e.exec("CREATE TABLE bench (id TEXT PRIMARY KEY, val TEXT)");

    run_phase("new",    mode, n, [&](size_t i) { e.exec("INSERT INTO bench VALUES (" + key(i) + ", 'v')"); },  m);
    run_phase("update", mode, n, [&](size_t i) { e.exec("UPDATE bench SET val = 'w' WHERE id = " + key(i)); },  m);
    run_phase("query",  mode, n, [&](size_t i) { e.exec("SELECT * FROM bench WHERE id = " + key(i)); },       m);
    run_phase("delete", mode, n, [&](size_t i) { e.exec("DELETE FROM bench WHERE id = " + key(i)); },       m);
    std::cout << "\n";
}

int main(int argc, char* argv[]) {
    size_t      N        = 10'000;
    std::string data_dir = "./bench_wal_data";
    std::string csv_path;
    Metrics     m;

    for (int i = 1, pos = 0; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--csv" && i + 1 < argc) csv_path = argv[++i];
        else if (pos++ == 0)              N        = static_cast<size_t>(std::stoul(a));
        else                              data_dir = a;
    }

    std::cout << "heavy-trie WAL bench  N=" << N << "  data_dir=" << data_dir << "\n\n";
    run_mode(true,  Wal::Sync::COMMIT, N, data_dir, m);
    run_mode(true,  Wal::Sync::NONE,   N, data_dir, m);
    run_mode(false, Wal::Sync::COMMIT, N, data_dir, m);

    m.write_csv(csv_path);
    return 0;
}
