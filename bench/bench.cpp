#include "catalog/table.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

// ---- helpers ----

static std::vector<uint8_t> encode_u64(uint64_t v) {
    std::vector<uint8_t> b(8);
    for (int i = 7; i >= 0; i--) { b[i] = v & 0xFF; v >>= 8; }
    return b;
}

static std::vector<uint8_t> encode_str(const std::string& s, uint16_t max_size) {
    std::vector<uint8_t> b(s.begin(), s.end());
    b.resize(max_size, 0);
    return b;
}

static Schema make_schema() {
    Schema s;
    s.table_name = "bench";
    s.pk_col     = 0;
    s.cols.push_back({"id",   ColType::UINT64,  8});
    s.cols.push_back({"val",  ColType::VARCHAR, 64});
    return s;
}

// ---- timing ----

using Clock = std::chrono::steady_clock;
using Dur   = std::chrono::duration<double>;

static double ops_per_sec(size_t n, double secs) {
    return secs > 0 ? n / secs : 0;
}

// ---- main ----

int main(int argc, char* argv[]) {
    size_t      N        = 100'000;
    std::string data_dir = "./bench_data";

    if (argc >= 2) N        = static_cast<size_t>(std::stoul(argv[1]));
    if (argc >= 3) data_dir = argv[2];

    std::cout << "heavy-trie bench  N=" << N
              << "  data_dir=" << data_dir << "\n\n";

    // Wipe and recreate data directory for a fresh run.
    std::filesystem::remove_all(data_dir);
    std::filesystem::create_directories(data_dir);

    Schema s = make_schema();

    // Generate N distinct random uint64 keys.
    std::mt19937_64 rng(42);
    std::vector<uint64_t> keys;
    keys.reserve(N);
    {
        std::uniform_int_distribution<uint64_t> dist;
        while (keys.size() < N) {
            uint64_t k = dist(rng);
            keys.push_back(k);
        }
    }

    // ---- BULK INSERT benchmark ----
    double bulk_secs;
    {
        Table tb = Table::create(s, data_dir + "/bulk.trie", data_dir + "/bulk.heap");
        std::vector<Row> bulk_rows;
        bulk_rows.reserve(N);
        for (uint64_t k : keys) {
            std::string val = "v" + std::to_string(k);
            bulk_rows.push_back({encode_u64(k), encode_str(val, 64)});
        }
        auto t0b = Clock::now();
        tb.bulk_insert(std::move(bulk_rows));
        bulk_secs = Dur(Clock::now() - t0b).count();
    }

    // ---- INSERT benchmark ----
    Table  t = Table::create(s, data_dir + "/bench.trie", data_dir + "/bench.heap");
    auto t0 = Clock::now();
    size_t inserted = 0;
    for (uint64_t k : keys) {
        std::string val = "v" + std::to_string(k);
        Row row = {encode_u64(k), encode_str(val, 64)};
        if (t.insert(row)) inserted++;
    }
    double insert_secs = Dur(Clock::now() - t0).count();

    // ---- LOOKUP benchmark ----
    // Shuffle keys so lookups are in random order.
    std::vector<uint64_t> lookup_keys = keys;
    std::shuffle(lookup_keys.begin(), lookup_keys.end(), rng);

    size_t found        = 0;
    size_t total_chains = 0;

    auto t1 = Clock::now();
    for (uint64_t k : lookup_keys) {
        size_t chains = 0;
        Row    row;
        if (t.lookup(encode_u64(k), &row, &chains)) {
            found++;
            total_chains += chains;
        }
    }
    double lookup_secs = Dur(Clock::now() - t1).count();

    // ---- RANGE benchmark: 100 random ranges, each spanning ~N/1000 keys ----
    // Use keys sorted to build meaningful lo/hi pairs.
    std::vector<uint64_t> sorted_keys = keys;
    std::sort(sorted_keys.begin(), sorted_keys.end());

    size_t range_total = 0;
    size_t range_ops   = std::min<size_t>(100, N / 10);
    std::uniform_int_distribution<size_t> idx_dist(0, N - 1);

    auto t2 = Clock::now();
    for (size_t r = 0; r < range_ops; r++) {
        size_t lo_idx = idx_dist(rng);
        size_t hi_idx = std::min(lo_idx + N / 1000, N - 1);
        uint64_t lo = sorted_keys[lo_idx];
        uint64_t hi = sorted_keys[hi_idx];
        auto rows = t.range(encode_u64(lo), encode_u64(hi));
        range_total += rows.size();
    }
    double range_secs = Dur(Clock::now() - t2).count();

    // ---- Results ----
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "BULK INSERT\n"
              << "  inserted     : " << N << " / " << N << "\n"
              << "  time         : " << bulk_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(N, bulk_secs) / 1000 << " K ops/sec\n\n";

    std::cout << "INSERT (single)\n"
              << "  inserted     : " << inserted << " / " << N << "\n"
              << "  time         : " << insert_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(inserted, insert_secs) / 1000 << " K ops/sec\n\n";

    std::cout << "LOOKUP (random order)\n"
              << "  found        : " << found << " / " << N << "\n"
              << "  time         : " << lookup_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(found, lookup_secs) / 1000 << " K ops/sec\n"
              << "  avg chains   : " << (found ? static_cast<double>(total_chains) / found : 0) << "\n\n";

    std::cout << "RANGE (" << range_ops << " scans, ~" << N / 1000 << " keys each)\n"
              << "  rows returned: " << range_total << "\n"
              << "  time         : " << range_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(range_ops, range_secs) << " scans/sec\n\n";

    std::cout << "TRIE STATS\n"
              << "  records      : " << t.record_count() << "\n"
              << "  active chains: " << t.active_chain_count() << "\n"
              << "  alloc chains : " << t.chain_count() << "\n";

    return 0;
}
