#pragma once
// Shared YCSB workload definitions, key generation, Zipfian generator,
// and timing helpers. Used by both ycsb.cpp (heavy-trie) and ycsb_pg.cpp (PostgreSQL).

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

// ---- constants ----

static constexpr int    FIELD_COUNT  = 10;
static constexpr int    FIELD_SIZE   = 100;
static constexpr double ZIPF_THETA   = 0.99;
static constexpr int    MAX_SCAN_LEN = 100;

// ---- timing ----

using Clock = std::chrono::steady_clock;
using Dur   = std::chrono::duration<double>;

// ---- key helpers ----

inline std::string make_key(uint64_t n) {
    char buf[20];
    snprintf(buf, sizeof(buf), "user%010llu", (unsigned long long)n);
    return {buf, 14};
}

// Random alphanumeric field value.
// Restricted to [0-9A-Za-z] so PostgreSQL COPY text format never sees
// backslash escape sequences (e.g. \x00, \000) that would produce null bytes.
inline std::string random_field_str(std::mt19937_64& rng) {
    static constexpr char ALPHA[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static std::uniform_int_distribution<int> dist(0, 61);
    std::string v(FIELD_SIZE, ' ');
    for (char& c : v) c = ALPHA[dist(rng)];
    return v;
}

// ---- Zipfian generator (Gray et al. 1994, scrambled variant) ----

class ZipfianGenerator {
public:
    explicit ZipfianGenerator(uint64_t n, double theta = ZIPF_THETA)
        : n_(n), theta_(theta)
    {
        zeta2_ = 1.0 + std::pow(0.5, theta_);
        zetan_ = compute_zeta(n_, theta_);
        eta_   = (1.0 - std::pow(2.0 / static_cast<double>(n_), 1.0 - theta_))
               / (1.0 - zeta2_ / zetan_);
    }

    uint64_t next(std::mt19937_64& rng) {
        std::uniform_real_distribution<double> u01(0.0, 1.0);
        double u  = u01(rng);
        double uz = u * zetan_;
        if (uz < 1.0)                          return 0;
        if (uz < 1.0 + std::pow(0.5, theta_)) return 1;
        uint64_t v = static_cast<uint64_t>(
            static_cast<double>(n_) *
            std::pow(eta_ * u - eta_ + 1.0, 1.0 / (1.0 - theta_)));
        return std::min(v, n_ - 1);
    }

    // Scramble: hash rank → random position in keyspace so hot items are scattered.
    uint64_t next_scrambled(std::mt19937_64& rng) {
        uint64_t v = next(rng);
        uint64_t h = 14695981039346656037ULL;
        for (int i = 0; i < 8; i++) { h ^= (v >> (i * 8)) & 0xFF; h *= 1099511628211ULL; }
        return h % n_;
    }

private:
    uint64_t n_;
    double   theta_, zeta2_, zetan_, eta_;

    static double compute_zeta(uint64_t n, double theta) {
        double sum = 0.0;
        for (uint64_t i = 1; i <= n; i++)
            sum += 1.0 / std::pow(static_cast<double>(i), theta);
        return sum;
    }
};

// ---- Latest generator (Workload D) ----

class LatestGenerator {
public:
    explicit LatestGenerator(uint64_t initial_n) : zipf_(initial_n) {}
    uint64_t next(std::mt19937_64& rng, uint64_t inserted) {
        uint64_t rank = zipf_.next(rng);
        return inserted > rank ? inserted - 1 - rank : 0;
    }
private:
    ZipfianGenerator zipf_;
};

// ---- per-operation stats ----

struct OpStats {
    uint64_t count      = 0;
    double   total_secs = 0;
    void   record(double s)      { count++; total_secs += s; }
    double throughput()    const { return count > 0 && total_secs > 0 ? count / total_secs : 0; }
    double avg_ms()        const { return count > 0 ? total_secs * 1000.0 / count : 0; }
};

struct WorkloadResult {
    OpStats read, update, insert, scan, rmw;
};

// ---- workload definition ----

enum class Workload { A, B, C, D, E, F };

struct WorkloadDef {
    Workload    id;
    const char* label;
    double      read_p, update_p, insert_p, scan_p, rmw_p;
    bool        use_latest;
};

inline const WorkloadDef& get_workload(Workload w) {
    static const WorkloadDef defs[] = {
        {Workload::A, "A", 0.50, 0.50, 0.00, 0.00, 0.00, false},
        {Workload::B, "B", 0.95, 0.05, 0.00, 0.00, 0.00, false},
        {Workload::C, "C", 1.00, 0.00, 0.00, 0.00, 0.00, false},
        {Workload::D, "D", 0.95, 0.00, 0.05, 0.00, 0.00, true },
        {Workload::E, "E", 0.00, 0.00, 0.05, 0.95, 0.00, false},
        {Workload::F, "F", 0.50, 0.00, 0.00, 0.00, 0.50, false},
    };
    return defs[static_cast<int>(w)];
}

// ---- result reporting ----

inline void print_op(const std::string& name, const OpStats& s) {
    if (s.count == 0) return;
    std::cout << "  " << std::left  << std::setw(8) << name
              << std::right
              << std::setw(10) << s.count           << " ops  "
              << std::setw(10) << std::fixed << std::setprecision(2)
              << s.throughput() / 1000.0             << " K/s  "
              << std::setw(8)  << s.avg_ms()         << " ms avg\n";
}

inline void print_result(const std::string& label, const WorkloadResult& res) {
    print_op("READ",   res.read);
    print_op("UPDATE", res.update);
    print_op("INSERT", res.insert);
    print_op("SCAN",   res.scan);
    print_op("RMW",    res.rmw);

    uint64_t total_ops = res.read.count + res.update.count + res.insert.count
                       + res.scan.count + res.rmw.count;
    double   total_s   = res.read.total_secs + res.update.total_secs
                       + res.insert.total_secs + res.scan.total_secs
                       + res.rmw.total_secs;
    if (total_s > 0)
        std::cout << "  OVERALL " << total_ops << " ops  "
                  << std::fixed << std::setprecision(2)
                  << total_ops / total_s / 1000 << " K/s\n";
}
