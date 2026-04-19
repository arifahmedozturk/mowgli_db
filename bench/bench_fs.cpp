#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

// Filesystem baseline: one folder per "table", one file per row.
// File name = decimal primary key. File content = "id=<k>\nval=<v>\n".
// Range scan = list directory, parse filenames, filter by [lo,hi], read matching files.

namespace fs = std::filesystem;

using Clock = std::chrono::steady_clock;
using Dur   = std::chrono::duration<double>;

static double ops_per_sec(size_t n, double secs) {
    return secs > 0 ? n / secs : 0;
}

static std::string row_path(const std::string& dir, uint64_t key) {
    return dir + "/" + std::to_string(key);
}

static void fs_write(const std::string& dir, uint64_t key, const std::string& val) {
    std::ofstream f(row_path(dir, key));
    f << "id=" << key << "\nval=" << val << "\n";
}

// Returns true if the file existed.
static bool fs_read(const std::string& dir, uint64_t key, std::string& val_out) {
    std::ifstream f(row_path(dir, key));
    if (!f) return false;
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("val=", 0) == 0)
            val_out = line.substr(4);
    }
    return true;
}

// Range scan: iterate directory entries, collect keys in [lo,hi], read each file.
static size_t fs_range(const std::string& dir, uint64_t lo, uint64_t hi) {
    size_t count = 0;
    for (auto& entry : fs::directory_iterator(dir)) {
        uint64_t k = std::stoull(entry.path().filename().string());
        if (k >= lo && k <= hi) {
            std::string val;
            fs_read(dir, k, val);
            count++;
        }
    }
    return count;
}

int main(int argc, char* argv[]) {
    size_t      N        = 100'000;
    std::string data_dir = "./bench_fs_data";

    if (argc >= 2) N        = static_cast<size_t>(std::stoul(argv[1]));
    if (argc >= 3) data_dir = argv[2];

    std::cout << "filesystem baseline  N=" << N
              << "  data_dir=" << data_dir << "\n\n";

    fs::remove_all(data_dir);

    // Single insert table.
    const std::string ins_dir = data_dir + "/insert";
    // Bulk insert table (same data, same folder structure).
    const std::string bulk_dir = data_dir + "/bulk";
    fs::create_directories(ins_dir);
    fs::create_directories(bulk_dir);

    std::mt19937_64 rng(42);
    std::vector<uint64_t> keys;
    keys.reserve(N);
    {
        std::uniform_int_distribution<uint64_t> dist;
        while (keys.size() < N)
            keys.push_back(dist(rng));
    }

    // ---- BULK INSERT ----
    auto tb0 = Clock::now();
    for (uint64_t k : keys)
        fs_write(bulk_dir, k, "v" + std::to_string(k));
    double bulk_secs = Dur(Clock::now() - tb0).count();

    // ---- INSERT (single) ----
    auto t0 = Clock::now();
    for (uint64_t k : keys)
        fs_write(ins_dir, k, "v" + std::to_string(k));
    double insert_secs = Dur(Clock::now() - t0).count();

    // ---- LOOKUP (random order) ----
    std::vector<uint64_t> lookup_keys = keys;
    std::shuffle(lookup_keys.begin(), lookup_keys.end(), rng);

    size_t found = 0;
    auto t1 = Clock::now();
    for (uint64_t k : lookup_keys) {
        std::string val;
        if (fs_read(ins_dir, k, val)) found++;
    }
    double lookup_secs = Dur(Clock::now() - t1).count();

    // ---- RANGE ----
    std::vector<uint64_t> sorted_keys = keys;
    std::sort(sorted_keys.begin(), sorted_keys.end());

    size_t range_total = 0;
    size_t range_ops   = std::min<size_t>(100, N / 10);
    std::uniform_int_distribution<size_t> idx_dist(0, N - 1);

    auto t2 = Clock::now();
    for (size_t r = 0; r < range_ops; r++) {
        size_t lo_idx = idx_dist(rng);
        size_t hi_idx = std::min(lo_idx + N / 1000, N - 1);
        range_total += fs_range(ins_dir, sorted_keys[lo_idx], sorted_keys[hi_idx]);
    }
    double range_secs = Dur(Clock::now() - t2).count();

    // ---- NARROW RANGE (point scans lo==hi) ----
    size_t narrow_total = 0;
    size_t narrow_ops   = 500;
    std::uniform_int_distribution<size_t> narrow_idx(0, N - 1);

    auto t3 = Clock::now();
    for (size_t r = 0; r < narrow_ops; r++) {
        uint64_t lo = sorted_keys[narrow_idx(rng)];
        narrow_total += fs_range(ins_dir, lo, lo);
    }
    double narrow_secs = Dur(Clock::now() - t3).count();

    // ---- Results ----
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "BULK INSERT\n"
              << "  inserted     : " << N << " / " << N << "\n"
              << "  time         : " << bulk_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(N, bulk_secs) / 1000 << " K ops/sec\n\n";

    std::cout << "INSERT (single)\n"
              << "  inserted     : " << N << " / " << N << "\n"
              << "  time         : " << insert_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(N, insert_secs) / 1000 << " K ops/sec\n\n";

    std::cout << "LOOKUP (random order)\n"
              << "  found        : " << found << " / " << N << "\n"
              << "  time         : " << lookup_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(found, lookup_secs) / 1000 << " K ops/sec\n\n";

    std::cout << "RANGE (" << range_ops << " scans, ~" << N / 1000 << " keys each)\n"
              << "  rows returned: " << range_total << "\n"
              << "  time         : " << range_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(range_ops, range_secs) << " scans/sec\n\n";

    std::cout << "RANGE NARROW (" << narrow_ops << " point scans, lo==hi)\n"
              << "  rows returned: " << narrow_total << "\n"
              << "  time         : " << narrow_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(narrow_ops, narrow_secs) << " scans/sec\n\n";

    // Sum file sizes in the insert directory.
    uintmax_t total_bytes = 0;
    for (auto& entry : fs::directory_iterator(ins_dir))
        if (fs::is_regular_file(entry))
            total_bytes += fs::file_size(entry);

    std::cout << "DISK USAGE\n"
              << "  insert dir   : " << (total_bytes < 1024 * 1024
                                         ? std::to_string(total_bytes / 1024) + " KB"
                                         : std::to_string(total_bytes / 1024 / 1024) + " MB") << "\n";

    return 0;
}
