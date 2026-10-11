#include "bench/keys.h"
#include "bench/metrics.h"
#include "catalog/table.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <unistd.h>

// ---- helpers ----

static std::vector<uint8_t> encode_str(const std::string& s, uint16_t max_size) {
    std::vector<uint8_t> b(s.begin(), s.end());
    b.resize(max_size, 0);
    return b;
}

static Schema make_schema(const std::string& dist) {
    Schema s;
    s.table_name = "bench";
    s.pk_col     = 0;
    if (is_u64_dist(dist)) s.cols.push_back({"id", ColType::UINT64,  8});
    else                   s.cols.push_back({"id", ColType::VARCHAR, STR_KEY_MAX});
    s.cols.push_back({"val",  ColType::VARCHAR, 64});
    return s;
}

// ---- timing ----

using Clock = std::chrono::steady_clock;
using Dur   = std::chrono::duration<double>;

static double ops_per_sec(size_t n, double secs) {
    return secs > 0 ? n / secs : 0;
}

// ---- per-lookup physical cost ----

// Average distinct index blocks pinned per lookup (untimed pass; hot-cache hits pin nothing).
static double blocks_per_lookup(Table& t, const std::vector<Key>& keys) {
    std::vector<uint64_t> trace;
    size_t total = 0;
    BufferPool::pin_trace = &trace;
    for (const Key& k : keys) {
        trace.clear();
        t.lookup(k);
        std::sort(trace.begin(), trace.end());
        total += std::unique(trace.begin(), trace.end()) - trace.begin();
    }
    BufferPool::pin_trace = nullptr;
    return keys.empty() ? 0 : double(total) / keys.size();
}

static double per_key(uint64_t bytes, size_t n) { return n ? double(bytes) / n : 0; }

// Copy a closed table's files, including the saved Bloom filter if present.
static void copy_table(const std::string& from, const std::string& to) {
    for (const char* ext : {".trie", ".heap", ".trie.bloom"})
        if (std::filesystem::exists(from + ext))
            std::filesystem::copy_file(from + ext, to + ext,
                                       std::filesystem::copy_options::overwrite_existing);
}

// ---- cold cache ----

// Drop a closed file's pages from the OS page cache (no root needed; the file
// must not be mapped, so close its Table first).
static void evict_from_page_cache(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return;
    ::fdatasync(fd);
    ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    ::close(fd);
}

// Fraction of a file's pages currently in the page cache.
static double resident_fraction(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return 0;
    size_t size = std::filesystem::file_size(path);
    double frac = 0;
    if (size > 0) {
        void* p = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (p != MAP_FAILED) {
            size_t page = ::sysconf(_SC_PAGESIZE), pages = (size + page - 1) / page;
            std::vector<unsigned char> vec(pages);
            if (::mincore(p, size, vec.data()) == 0) {
                size_t in = 0;
                for (unsigned char v : vec) in += v & 1;
                frac = double(in) / pages;
            }
            ::munmap(p, size);
        }
    }
    ::close(fd);
    return frac;
}

static long major_faults() {
    rusage ru{};
    ::getrusage(RUSAGE_SELF, &ru);
    return ru.ru_majflt;
}

static constexpr size_t COLD_OPS = 1000;

// Cold start: evict the table's files, reopen (timed), then time the first
// COLD_OPS row-fetching lookups. Note Table::open walks the whole index
// (counts, Bloom filter, packed-block state), so the index is warm again by the
// first lookup and lookup faults come from the heap; the cold index cost shows
// up in startup.
static void cold_run(const std::string& label, const Schema& s, const std::string& base,
                     const std::vector<Key>& lookup_keys, Metrics& m) {
    std::string trie = base + ".trie", heap = base + ".heap";
    evict_from_page_cache(trie);
    evict_from_page_cache(heap);
    evict_from_page_cache(trie + ".bloom");
    double resident = (resident_fraction(trie) + resident_fraction(heap)) / 2;

    long   flt0 = major_faults();
    auto   ts0  = Clock::now();
    Table  t    = Table::open(s, trie, heap);
    double open_secs = Dur(Clock::now() - ts0).count();

    size_t    ops = std::min(COLD_OPS, lookup_keys.size()), found = 0;
    long      flt1 = major_faults();
    Latencies lat;
    auto t0 = Clock::now();
    for (size_t i = 0; i < ops; i++) {
        Row  row;
        auto tl = Clock::now();
        if (t.lookup(lookup_keys[i], &row)) found++;
        lat.record(Dur(Clock::now() - tl).count());
    }
    double secs   = Dur(Clock::now() - t0).count();
    double faults = ops ? double(major_faults() - flt1) / ops : 0;

    std::cout << "COLD START — " << label << " (open + first " << ops << " lookups after page-cache eviction)\n"
              << "  resident     : " << resident * 100 << " % before open\n"
              << "  startup      : " << open_secs * 1000 << " ms  (" << (flt1 - flt0) << " major faults)\n"
              << "  found        : " << found << " / " << ops << "\n"
              << "  throughput   : " << ops_per_sec(ops, secs) / 1000 << " K ops/sec\n"
              << "  major faults : " << faults << " per lookup\n"
              << "  p50/p99/p999 : " << lat.p_us(0.50) << " / " << lat.p_us(0.99)
              << " / " << lat.p_us(0.999) << " us\n\n";

    std::string p = "cold_" + label;
    m.add(p + "_startup_time",  open_secs * 1000,        "ms");
    m.add(p + "_lookup",        ops_per_sec(ops, secs),  "ops/s");
    m.add(p + "_lookup_faults", faults,                  "faults");
    m.add_latency(p + "_lookup", lat);
}

// ---- main ----

int main(int argc, char* argv[]) {
    // Usage: bench [N] [data_dir] [--csv file] [--keys random_u64|seq_u64|prefix_str|varlen_str]
    size_t      N        = 100'000;
    std::string data_dir = "./bench_data";
    std::string csv_path;
    std::string key_dist = "random_u64";
    Metrics     m;

    for (int i = 1, pos = 0; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--csv" && i + 1 < argc)       csv_path = argv[++i];
        else if (a == "--keys" && i + 1 < argc) key_dist = argv[++i];
        else if (pos++ == 0)              N        = static_cast<size_t>(std::stoul(a));
        else                              data_dir = a;
    }

    std::cout << "heavy-trie bench  N=" << N << "  keys=" << key_dist
              << "  data_dir=" << data_dir << "\n\n";

    // Wipe and recreate data directory for a fresh run.
    std::filesystem::remove_all(data_dir);
    std::filesystem::create_directories(data_dir);

    Schema s = make_schema(key_dist);

    std::mt19937_64 rng(42);
    std::vector<Key> keys = make_keys(key_dist, N, rng);

    // ---- BULK INSERT benchmark ----
    double bulk_secs;
    {
        Table tb = Table::create(s, data_dir + "/bulk.trie", data_dir + "/bulk.heap");
        std::vector<Row> bulk_rows;
        bulk_rows.reserve(N);
        for (size_t i = 0; i < N; i++)
            bulk_rows.push_back({keys[i], encode_str("v" + std::to_string(i), 64)});
        auto t0b = Clock::now();
        tb.bulk_insert(std::move(bulk_rows));
        bulk_secs = Dur(Clock::now() - t0b).count();
    }

    // ---- INSERT benchmark ----
    Table  t = Table::create(s, data_dir + "/bench.trie", data_dir + "/bench.heap");
    auto t0 = Clock::now();
    size_t inserted = 0;
    Latencies insert_lat;
    for (size_t i = 0; i < N; i++) {
        Row row = {keys[i], encode_str("v" + std::to_string(i), 64)};
        auto ti = Clock::now();
        if (t.insert(row)) inserted++;
        insert_lat.record(Dur(Clock::now() - ti).count());
    }
    double insert_secs = Dur(Clock::now() - t0).count();

    // ---- LOOKUP benchmark ----
    // Shuffle keys so lookups are in random order.
    std::vector<Key> lookup_keys = keys;
    std::shuffle(lookup_keys.begin(), lookup_keys.end(), rng);

    size_t found        = 0;
    size_t total_chains = 0;
    Latencies lookup_lat;

    auto t1 = Clock::now();
    for (const Key& k : lookup_keys) {
        size_t chains = 0;
        Row    row;
        auto   tl = Clock::now();
        bool   ok = t.lookup(k, &row, &chains);
        lookup_lat.record(Dur(Clock::now() - tl).count());
        if (ok) {
            found++;
            total_chains += chains;
        }
    }
    double lookup_secs   = Dur(Clock::now() - t1).count();
    double lookup_blocks = blocks_per_lookup(t, lookup_keys);

    // ---- RANGE benchmark: 100 random ranges, each spanning ~N/1000 keys ----
    std::vector<Key> sorted_keys = keys;
    std::sort(sorted_keys.begin(), sorted_keys.end());

    size_t range_total = 0;
    size_t range_ops   = std::min<size_t>(100, N / 10);
    std::uniform_int_distribution<size_t> idx_dist(0, N - 1);

    auto t2 = Clock::now();
    for (size_t r = 0; r < range_ops; r++) {
        size_t lo_idx = idx_dist(rng);
        size_t hi_idx = std::min(lo_idx + N / 1000, N - 1);
        const Key& lo = sorted_keys[lo_idx];
        const Key& hi = sorted_keys[hi_idx];
        auto rows = t.range(lo, hi);
        range_total += rows.size();
    }
    double range_secs = Dur(Clock::now() - t2).count();

    // ---- NARROW RANGE benchmark: 500 point-like ranges (lo == hi) ----
    size_t narrow_total = 0;
    size_t narrow_ops   = 500;
    std::uniform_int_distribution<size_t> narrow_idx(0, N - 1);

    auto t3 = Clock::now();
    for (size_t r = 0; r < narrow_ops; r++) {
        const Key& lo = sorted_keys[narrow_idx(rng)];
        const Key& hi = lo;
        auto rows = t.range(lo, hi);
        narrow_total += rows.size();
    }
    double narrow_secs = Dur(Clock::now() - t3).count();

    // ---- Results ----
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "BULK INSERT\n"
              << "  inserted     : " << N << " / " << N << "\n"
              << "  time         : " << bulk_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(N, bulk_secs) / 1000 << " K ops/sec\n\n";

    std::cout << "INSERT (single)\n"
              << "  inserted     : " << inserted << " / " << N << "\n"
              << "  time         : " << insert_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(inserted, insert_secs) / 1000 << " K ops/sec\n"
              << "  p50/p99/p999 : " << insert_lat.p_us(0.50) << " / " << insert_lat.p_us(0.99)
              << " / " << insert_lat.p_us(0.999) << " us\n\n";

    std::cout << "LOOKUP (random order)\n"
              << "  found        : " << found << " / " << N << "\n"
              << "  time         : " << lookup_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(found, lookup_secs) / 1000 << " K ops/sec\n"
              << "  avg chains   : " << (found ? static_cast<double>(total_chains) / found : 0) << "\n"
              << "  avg blocks   : " << lookup_blocks << "\n"
              << "  p50/p99/p999 : " << lookup_lat.p_us(0.50) << " / " << lookup_lat.p_us(0.99)
              << " / " << lookup_lat.p_us(0.999) << " us\n\n";

    std::cout << "RANGE (" << range_ops << " scans, ~" << N / 1000 << " keys each)\n"
              << "  rows returned: " << range_total << "\n"
              << "  time         : " << range_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(range_ops, range_secs) << " scans/sec\n\n";

    std::cout << "RANGE NARROW (" << narrow_ops << " point scans, lo==hi)\n"
              << "  rows returned: " << narrow_total << "\n"
              << "  time         : " << narrow_secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(narrow_ops, narrow_secs) << " scans/sec\n\n";

    std::cout << "TRIE STATS\n"
              << "  records      : " << t.record_count() << "\n"
              << "  active chains: " << t.active_chain_count() << "\n"
              << "  alloc chains : " << t.chain_count() << "\n"
              << "  index B/key  : " << per_key(t.index_bytes(), N) << "\n"
              << "  heap B/key   : " << per_key(t.heap_bytes(), N) << "\n\n";

    m.add("bulk_insert",   ops_per_sec(N, bulk_secs),                 "ops/s");
    m.add("insert",        ops_per_sec(inserted, insert_secs),        "ops/s");
    m.add_latency("insert", insert_lat);
    m.add("lookup",        ops_per_sec(found, lookup_secs),           "ops/s");
    m.add_latency("lookup", lookup_lat);
    m.add("lookup_chains", found ? double(total_chains) / found : 0,  "chains");
    m.add("lookup_blocks", lookup_blocks,                             "blocks");
    m.add("range",         ops_per_sec(range_ops, range_secs),        "scans/s");
    m.add("range_narrow",  ops_per_sec(narrow_ops, narrow_secs),      "scans/s");
    m.add("active_chains", double(t.active_chain_count()),            "chains");
    m.add("alloc_chains",  double(t.chain_count()),                   "chains");
    m.add("index_bytes_per_key", per_key(t.index_bytes(), N),         "bytes");
    m.add("heap_bytes_per_key",  per_key(t.heap_bytes(), N),          "bytes");

    // ---- COMPACT benchmark — same bulk table, same keys, before vs after ----
    // Fix the range scan indices so both runs hit identical key ranges.
    std::vector<std::pair<Key, Key>> range_pairs;
    range_pairs.reserve(range_ops);
    {
        std::mt19937_64 rng2(99); // independent seed — same for both runs
        std::uniform_int_distribution<size_t> ridx(0, N - 1);
        for (size_t r = 0; r < range_ops; r++) {
            size_t lo_idx = ridx(rng2);
            size_t hi_idx = std::min(lo_idx + N / 1000, N - 1);
            range_pairs.push_back({sorted_keys[lo_idx], sorted_keys[hi_idx]});
        }
    }

    // Untouched copy of the bulk table for the cold pre-compact run below.
    copy_table(data_dir + "/bulk", data_dir + "/bulkcold");

    {
        auto  ts0 = Clock::now();
        Table tb  = Table::open(s, data_dir + "/bulk.trie", data_dir + "/bulk.heap");
        double startup_secs = Dur(Clock::now() - ts0).count();

        // Pre-compact reads on the bulk table.
        size_t pre_found = 0, pre_chains = 0;
        auto tpre0 = Clock::now();
        for (const Key& k : lookup_keys) {
            size_t ch = 0; Row row;
            if (tb.lookup(k, &row, &ch)) { pre_found++; pre_chains += ch; }
        }
        double pre_lookup_secs  = Dur(Clock::now() - tpre0).count();
        double pre_blocks       = blocks_per_lookup(tb, lookup_keys);
        uint64_t pre_index_bytes = tb.index_bytes();

        size_t pre_range_total = 0;
        auto tpre1 = Clock::now();
        for (auto& [lo, hi] : range_pairs) {
            auto rows = tb.range(lo, hi);
            pre_range_total += rows.size();
        }
        double pre_range_secs = Dur(Clock::now() - tpre1).count();

        // Compact.
        auto tc0 = Clock::now();
        tb.compact();
        double compact_secs = Dur(Clock::now() - tc0).count();

        // Post-compact reads — same keys/ranges.
        size_t post_found = 0, post_chains = 0;
        auto tpost0 = Clock::now();
        for (const Key& k : lookup_keys) {
            size_t ch = 0; Row row;
            if (tb.lookup(k, &row, &ch)) { post_found++; post_chains += ch; }
        }
        double post_lookup_secs = Dur(Clock::now() - tpost0).count();
        double post_blocks      = blocks_per_lookup(tb, lookup_keys);

        size_t post_range_total = 0;
        auto tpost1 = Clock::now();
        for (auto& [lo, hi] : range_pairs) {
            auto rows = tb.range(lo, hi);
            post_range_total += rows.size();
        }
        double post_range_secs = Dur(Clock::now() - tpost1).count();

        std::cout << "STARTUP (open bulk table)\n"
                  << "  time         : " << startup_secs * 1000 << " ms\n\n";

        std::cout << "COMPACT\n"
                  << "  time         : " << compact_secs * 1000 << " ms\n"
                  << "  alloc chains : " << tb.chain_count() << "\n"
                  << "  index B/key  : " << per_key(pre_index_bytes, N) << " -> "
                  << per_key(tb.index_bytes(), N) << "\n\n";

        std::cout << "LOOKUP — pre-compact  (bulk table, warm cache)\n"
                  << "  throughput   : " << ops_per_sec(pre_found,  pre_lookup_secs)  / 1000 << " K ops/sec\n"
                  << "  avg chains   : " << (pre_found  ? static_cast<double>(pre_chains)  / pre_found  : 0) << "\n"
                  << "  avg blocks   : " << pre_blocks << "\n";
        std::cout << "LOOKUP — post-compact (bulk table, warm cache)\n"
                  << "  throughput   : " << ops_per_sec(post_found, post_lookup_secs) / 1000 << " K ops/sec\n"
                  << "  avg chains   : " << (post_found ? static_cast<double>(post_chains) / post_found : 0) << "\n"
                  << "  avg blocks   : " << post_blocks << "\n\n";

        std::cout << "RANGE  — pre-compact  (" << range_ops << " scans, ~" << N/1000 << " keys each)\n"
                  << "  rows returned: " << pre_range_total << "\n"
                  << "  throughput   : " << ops_per_sec(range_ops, pre_range_secs) << " scans/sec\n";
        std::cout << "RANGE  — post-compact (" << range_ops << " scans, ~" << N/1000 << " keys each)\n"
                  << "  rows returned: " << post_range_total << "\n"
                  << "  throughput   : " << ops_per_sec(range_ops, post_range_secs) << " scans/sec\n";

        m.add("startup_time",        startup_secs * 1000,                          "ms");
        m.add("compact_time",        compact_secs * 1000,                          "ms");
        m.add("compact_alloc_chains", double(tb.chain_count()),                    "chains");
        m.add("lookup_pre_compact",  ops_per_sec(pre_found, pre_lookup_secs),      "ops/s");
        m.add("lookup_post_compact", ops_per_sec(post_found, post_lookup_secs),    "ops/s");
        m.add("lookup_chains_pre_compact",  pre_found  ? double(pre_chains)  / pre_found  : 0, "chains");
        m.add("lookup_chains_post_compact", post_found ? double(post_chains) / post_found : 0, "chains");
        m.add("lookup_blocks_pre_compact",  pre_blocks,                            "blocks");
        m.add("lookup_blocks_post_compact", post_blocks,                           "blocks");
        m.add("index_bytes_per_key_pre_compact",  per_key(pre_index_bytes, N),     "bytes");
        m.add("index_bytes_per_key_post_compact", per_key(tb.index_bytes(), N),    "bytes");
        m.add("range_pre_compact",   ops_per_sec(range_ops, pre_range_secs),       "scans/s");
        m.add("range_post_compact",  ops_per_sec(range_ops, post_range_secs),      "scans/s");
    }

    // ---- COMPACT LEX benchmark — fresh bulk table, same keys/ranges ----
    std::cout << "\n";
    {
        // Copy the original bulk files so compact_lex starts from the same state
        // as compact() did above.
        copy_table(data_dir + "/bulk", data_dir + "/bulklex");

        Table tb = Table::open(s, data_dir + "/bulklex.trie", data_dir + "/bulklex.heap");

        auto tc0 = Clock::now();
        tb.compact_lex();
        double compact_lex_secs = Dur(Clock::now() - tc0).count();

        size_t lex_found = 0, lex_chains = 0;
        auto tlex0 = Clock::now();
        for (const Key& k : lookup_keys) {
            size_t ch = 0; Row row;
            if (tb.lookup(k, &row, &ch)) { lex_found++; lex_chains += ch; }
        }
        double lex_lookup_secs = Dur(Clock::now() - tlex0).count();
        double lex_blocks      = blocks_per_lookup(tb, lookup_keys);

        size_t lex_range_total = 0;
        auto tlex1 = Clock::now();
        for (auto& [lo, hi] : range_pairs) {
            auto rows = tb.range(lo, hi);
            lex_range_total += rows.size();
        }
        double lex_range_secs = Dur(Clock::now() - tlex1).count();

        std::cout << "COMPACT LEX\n"
                  << "  time         : " << compact_lex_secs * 1000 << " ms\n"
                  << "  alloc chains : " << tb.chain_count() << "\n"
                  << "  index B/key  : " << per_key(tb.index_bytes(), N) << "\n\n";

        std::cout << "LOOKUP — post-compact-lex (bulk table, warm cache)\n"
                  << "  throughput   : " << ops_per_sec(lex_found, lex_lookup_secs) / 1000 << " K ops/sec\n"
                  << "  avg chains   : " << (lex_found ? static_cast<double>(lex_chains) / lex_found : 0) << "\n"
                  << "  avg blocks   : " << lex_blocks << "\n\n";

        std::cout << "RANGE  — post-compact-lex (" << range_ops << " scans, ~" << N/1000 << " keys each)\n"
                  << "  rows returned: " << lex_range_total << "\n"
                  << "  throughput   : " << ops_per_sec(range_ops, lex_range_secs) << " scans/sec\n";

        m.add("compact_lex_time",         compact_lex_secs * 1000,                 "ms");
        m.add("compact_lex_alloc_chains", double(tb.chain_count()),                "chains");
        m.add("lookup_post_compact_lex",  ops_per_sec(lex_found, lex_lookup_secs), "ops/s");
        m.add("lookup_chains_post_compact_lex", lex_found ? double(lex_chains) / lex_found : 0, "chains");
        m.add("lookup_blocks_post_compact_lex", lex_blocks,                        "blocks");
        m.add("index_bytes_per_key_post_compact_lex", per_key(tb.index_bytes(), N), "bytes");
        m.add("range_post_compact_lex",   ops_per_sec(range_ops, lex_range_secs),  "scans/s");
    }

    // ---- COLD START — same lookups, starting from an evicted page cache ----
    std::cout << "\n";
    cold_run("pre_compact",      s, data_dir + "/bulkcold", lookup_keys, m);
    cold_run("post_compact",     s, data_dir + "/bulk",     lookup_keys, m);
    cold_run("post_compact_lex", s, data_dir + "/bulklex",  lookup_keys, m);

    m.write_csv(csv_path);
    return 0;
}
