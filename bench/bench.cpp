// heavy-trie benchmark: bulk load, single inserts, lookups (hits and misses),
// range scans, churn, compaction and cold starts on one key distribution.
// Each phase is a function below; main runs them in order. Metric names are
// what bench/run.sh aggregates and compare.py diffs, so keep them stable.
#include "bench/bench_common.h"
#include "bench/cold.h"
#include "bench/keys.h"
#include "bench/metrics.h"
#include "catalog/table.h"
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <utility>
#include <vector>

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

// Copy a closed table's files, including the saved Bloom filter if present.
static void copy_table(const std::string& from, const std::string& to) {
    for (const char* ext : {".trie", ".heap", ".trie.bloom"})
        if (std::filesystem::exists(from + ext))
            std::filesystem::copy_file(from + ext, to + ext,
                                       std::filesystem::copy_options::overwrite_existing);
}

// Row-fetching lookups of keys, optionally recording each one's latency.
struct LookupStats { size_t found = 0, chains = 0; double secs = 0; };

static LookupStats measure_lookups(Table& t, const std::vector<Key>& keys, Latencies* lat = nullptr) {
    LookupStats s;
    auto t0 = Clock::now();
    for (const Key& k : keys) {
        size_t chains = 0;
        Row    row;
        bool   ok;
        if (lat) {
            auto tl = Clock::now();
            ok = t.lookup(k, &row, &chains);
            lat->record(Dur(Clock::now() - tl).count());
        } else {
            ok = t.lookup(k, &row, &chains);
        }
        if (ok) { s.found++; s.chains += chains; }
    }
    s.secs = Dur(Clock::now() - t0).count();
    return s;
}

static double avg_chains(const LookupStats& s) { return s.found ? double(s.chains) / s.found : 0; }

struct RangeStats { size_t rows = 0; double secs = 0; };

static RangeStats measure_ranges(Table& t, const std::vector<std::pair<Key, Key>>& ranges) {
    RangeStats s;
    auto t0 = Clock::now();
    for (const auto& [lo, hi] : ranges) s.rows += t.range(lo, hi).size();
    s.secs = Dur(Clock::now() - t0).count();
    return s;
}

// ---- cold cache ----

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

// ---- one run ----

// The keys, data directory and random sequence the phases share. rng is drawn
// from in a fixed order (keys, lookup order, range picks), so runs repeat.
struct Run {
    size_t           N = 100'000;
    std::string      data_dir = "./bench_data";
    std::string      key_dist = "random_u64";
    Schema           schema;
    std::mt19937_64  rng{42};
    std::vector<Key> keys, lookup_keys, sorted_keys;
    size_t           range_ops = 0;   // range scans per phase, ~N/1000 keys each
    std::vector<std::pair<Key, Key>> bulk_ranges;   // the same ranges for every bulk-table phase
    Metrics          m;
};

// ~N/1000-key ranges starting at random sorted positions.
static std::vector<std::pair<Key, Key>> pick_ranges(const Run& r, std::mt19937_64& rng) {
    std::uniform_int_distribution<size_t> idx(0, r.N - 1);
    std::vector<std::pair<Key, Key>> ranges;
    ranges.reserve(r.range_ops);
    for (size_t i = 0; i < r.range_ops; i++) {
        size_t lo = idx(rng), hi = std::min(lo + r.N / 1000, r.N - 1);
        ranges.push_back({r.sorted_keys[lo], r.sorted_keys[hi]});
    }
    return ranges;
}

// Bulk load into an empty table (bulk.trie / bulk.heap, reused by the
// compaction and cold phases). Returns the seconds taken.
static double phase_bulk(Run& r) {
    Table tb = Table::create(r.schema, r.data_dir + "/bulk.trie", r.data_dir + "/bulk.heap");
    std::vector<Row> rows;
    rows.reserve(r.N);
    for (size_t i = 0; i < r.N; i++)
        rows.push_back({r.keys[i], encode_str("v" + std::to_string(i), 64)});
    auto t0 = Clock::now();
    tb.bulk_insert(std::move(rows));
    return Dur(Clock::now() - t0).count();
}

// Single inserts, lookups (hits and misses) and range scans on a table built
// one insert at a time; the table is left for the churn phase.
static void phase_single(Run& r, Table& t, double bulk_secs) {
    const size_t N = r.N;
    size_t    inserted = 0;
    Latencies insert_lat;
    auto t0 = Clock::now();
    for (size_t i = 0; i < N; i++) {
        Row row = {r.keys[i], encode_str("v" + std::to_string(i), 64)};
        auto ti = Clock::now();
        if (t.insert(row)) inserted++;
        insert_lat.record(Dur(Clock::now() - ti).count());
    }
    double insert_secs = Dur(Clock::now() - t0).count();

    // Lookups in random order.
    r.lookup_keys = r.keys;
    std::shuffle(r.lookup_keys.begin(), r.lookup_keys.end(), r.rng);
    Latencies   lookup_lat;
    LookupStats lk            = measure_lookups(t, r.lookup_keys, &lookup_lat);
    double      lookup_blocks = blocks_per_lookup(t, r.lookup_keys);

    // Missing keys: each stored key with a byte appended (absent from the u64
    // sets; the rare hit in the string sets is counted). Shows what the Bloom
    // filter saves.
    size_t missing_ops = std::min<size_t>(N, 200000), missing_hits = 0;
    std::vector<Key> missing_keys(r.lookup_keys.begin(), r.lookup_keys.begin() + static_cast<ptrdiff_t>(missing_ops));
    for (Key& k : missing_keys) k.push_back(0x01);
    auto t1m = Clock::now();
    for (const Key& k : missing_keys) missing_hits += t.lookup(k, nullptr, nullptr);
    double missing_secs = Dur(Clock::now() - t1m).count();

    // Range scans, then point-like ranges (lo == hi).
    r.sorted_keys = r.keys;
    std::sort(r.sorted_keys.begin(), r.sorted_keys.end());
    r.range_ops = std::min<size_t>(100, N / 10);
    RangeStats range = measure_ranges(t, pick_ranges(r, r.rng));

    const size_t narrow_ops = 500;
    std::uniform_int_distribution<size_t> narrow_idx(0, N - 1);
    std::vector<std::pair<Key, Key>> points;
    for (size_t i = 0; i < narrow_ops; i++) {
        const Key& k = r.sorted_keys[narrow_idx(r.rng)];
        points.push_back({k, k});
    }
    RangeStats narrow = measure_ranges(t, points);

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

    std::cout << "LOOKUP MISSING (keys not in the table)\n"
              << "  throughput   : " << ops_per_sec(missing_ops, missing_secs) / 1000 << " K ops/sec"
              << "   (" << missing_hits << " found)\n\n";

    std::cout << "LOOKUP (random order)\n"
              << "  found        : " << lk.found << " / " << N << "\n"
              << "  time         : " << lk.secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(lk.found, lk.secs) / 1000 << " K ops/sec\n"
              << "  avg chains   : " << avg_chains(lk) << "\n"
              << "  avg blocks   : " << lookup_blocks << "\n"
              << "  p50/p99/p999 : " << lookup_lat.p_us(0.50) << " / " << lookup_lat.p_us(0.99)
              << " / " << lookup_lat.p_us(0.999) << " us\n\n";

    std::cout << "RANGE (" << r.range_ops << " scans, ~" << N / 1000 << " keys each)\n"
              << "  rows returned: " << range.rows << "\n"
              << "  time         : " << range.secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(r.range_ops, range.secs) << " scans/sec\n\n";

    std::cout << "RANGE NARROW (" << narrow_ops << " point scans, lo==hi)\n"
              << "  rows returned: " << narrow.rows << "\n"
              << "  time         : " << narrow.secs * 1000 << " ms\n"
              << "  throughput   : " << ops_per_sec(narrow_ops, narrow.secs) << " scans/sec\n\n";

    std::cout << "TRIE STATS\n"
              << "  records      : " << t.record_count() << "\n"
              << "  active chains: " << t.active_chain_count() << "\n"
              << "  alloc chains : " << t.chain_count() << "\n"
              << "  index B/key  : " << per_key(t.index_bytes(), N) << "\n"
              << "  heap B/key   : " << per_key(t.heap_bytes(), N) << "\n\n";

    Metrics& m = r.m;
    m.add("bulk_insert",   ops_per_sec(N, bulk_secs),                 "ops/s");
    m.add("insert",        ops_per_sec(inserted, insert_secs),        "ops/s");
    m.add_latency("insert", insert_lat);
    m.add("lookup",        ops_per_sec(lk.found, lk.secs),            "ops/s");
    m.add_latency("lookup", lookup_lat);
    m.add("lookup_missing", ops_per_sec(missing_ops, missing_secs),   "ops/s");
    m.add("lookup_chains", avg_chains(lk),                            "chains");
    m.add("lookup_blocks", lookup_blocks,                             "blocks");
    m.add("range",         ops_per_sec(r.range_ops, range.secs),      "scans/s");
    m.add("range_narrow",  ops_per_sec(narrow_ops, narrow.secs),      "scans/s");
    m.add("active_chains", double(t.active_chain_count()),            "chains");
    m.add("alloc_chains",  double(t.chain_count()),                   "chains");
    m.add("index_bytes_per_key", per_key(t.index_bytes(), N),         "bytes");
    m.add("heap_bytes_per_key",  per_key(t.heap_bytes(), N),          "bytes");
}

// Remove every other key, then insert as many new ones: delete/insert speed,
// and whether freed index and heap space is reused (#19), so bytes per live
// key should stay close to the figures above.
static void phase_churn(Run& r, Table& t) {
    std::mt19937_64 churn_rng(7);
    auto fresh_key = [&](size_t i) -> Key {
        if (is_u64_dist(r.key_dist))
            return encode_u64(r.key_dist == "seq_u64" ? r.N + i : churn_rng());
        Key k = r.keys[i];
        k.back() = '~';   // never in the generated alphabets, so not an existing key
        return k;
    };
    size_t removed = 0, added = 0;
    auto tr = Clock::now();
    for (size_t i = 0; i < r.N; i += 2) removed += t.remove(r.keys[i]);
    double remove_secs = Dur(Clock::now() - tr).count();
    auto ti = Clock::now();
    for (size_t i = 0; i < r.N; i += 2)
        added += t.insert({fresh_key(i), encode_str("c" + std::to_string(i), 64)});
    double insert_secs = Dur(Clock::now() - ti).count();
    size_t live = t.record_count();

    std::cout << "CHURN (remove every other key, insert as many new ones)\n"
              << "  removed      : " << removed << " at " << ops_per_sec(removed, remove_secs) / 1000 << " K ops/sec\n"
              << "  inserted     : " << added << " at " << ops_per_sec(added, insert_secs) / 1000 << " K ops/sec\n"
              << "  index B/key  : " << per_key(t.index_bytes(), live) << "\n"
              << "  heap B/key   : " << per_key(t.heap_bytes(), live) << "\n\n";
    r.m.add("churn_remove",              ops_per_sec(removed, remove_secs), "ops/s");
    r.m.add("churn_insert",              ops_per_sec(added, insert_secs),   "ops/s");
    r.m.add("index_bytes_per_key_churn", per_key(t.index_bytes(), live),    "bytes");
    r.m.add("heap_bytes_per_key_churn",  per_key(t.heap_bytes(), live),     "bytes");
}

// The bulk table before and after compact(): startup, lookups, ranges.
static void phase_compact(Run& r) {
    const size_t N = r.N;
    {
        std::mt19937_64 rng2(99); // independent seed: the same ranges for every bulk-table phase
        r.bulk_ranges = pick_ranges(r, rng2);
    }
    // Untouched copy of the bulk table for the cold pre-compact run below.
    copy_table(r.data_dir + "/bulk", r.data_dir + "/bulkcold");

    auto   ts0 = Clock::now();
    Table  tb  = Table::open(r.schema, r.data_dir + "/bulk.trie", r.data_dir + "/bulk.heap");
    double startup_secs = Dur(Clock::now() - ts0).count();

    LookupStats pre             = measure_lookups(tb, r.lookup_keys);
    double      pre_blocks      = blocks_per_lookup(tb, r.lookup_keys);
    uint64_t    pre_index_bytes = tb.index_bytes();
    RangeStats  pre_range       = measure_ranges(tb, r.bulk_ranges);

    auto tc0 = Clock::now();
    tb.compact();
    double compact_secs = Dur(Clock::now() - tc0).count();

    LookupStats post        = measure_lookups(tb, r.lookup_keys);
    double      post_blocks = blocks_per_lookup(tb, r.lookup_keys);
    RangeStats  post_range  = measure_ranges(tb, r.bulk_ranges);

    std::cout << "STARTUP (open bulk table)\n"
              << "  time         : " << startup_secs * 1000 << " ms\n\n";

    std::cout << "COMPACT\n"
              << "  time         : " << compact_secs * 1000 << " ms\n"
              << "  alloc chains : " << tb.chain_count() << "\n"
              << "  index B/key  : " << per_key(pre_index_bytes, N) << " -> "
              << per_key(tb.index_bytes(), N) << "\n\n";

    std::cout << "LOOKUP — pre-compact  (bulk table, warm cache)\n"
              << "  throughput   : " << ops_per_sec(pre.found,  pre.secs)  / 1000 << " K ops/sec\n"
              << "  avg chains   : " << avg_chains(pre) << "\n"
              << "  avg blocks   : " << pre_blocks << "\n";
    std::cout << "LOOKUP — post-compact (bulk table, warm cache)\n"
              << "  throughput   : " << ops_per_sec(post.found, post.secs) / 1000 << " K ops/sec\n"
              << "  avg chains   : " << avg_chains(post) << "\n"
              << "  avg blocks   : " << post_blocks << "\n\n";

    std::cout << "RANGE  — pre-compact  (" << r.range_ops << " scans, ~" << N/1000 << " keys each)\n"
              << "  rows returned: " << pre_range.rows << "\n"
              << "  throughput   : " << ops_per_sec(r.range_ops, pre_range.secs) << " scans/sec\n";
    std::cout << "RANGE  — post-compact (" << r.range_ops << " scans, ~" << N/1000 << " keys each)\n"
              << "  rows returned: " << post_range.rows << "\n"
              << "  throughput   : " << ops_per_sec(r.range_ops, post_range.secs) << " scans/sec\n";

    Metrics& m = r.m;
    m.add("startup_time",        startup_secs * 1000,                          "ms");
    m.add("compact_time",        compact_secs * 1000,                          "ms");
    m.add("compact_alloc_chains", double(tb.chain_count()),                    "chains");
    m.add("lookup_pre_compact",  ops_per_sec(pre.found, pre.secs),             "ops/s");
    m.add("lookup_post_compact", ops_per_sec(post.found, post.secs),           "ops/s");
    m.add("lookup_chains_pre_compact",  avg_chains(pre),                       "chains");
    m.add("lookup_chains_post_compact", avg_chains(post),                      "chains");
    m.add("lookup_blocks_pre_compact",  pre_blocks,                            "blocks");
    m.add("lookup_blocks_post_compact", post_blocks,                           "blocks");
    m.add("index_bytes_per_key_pre_compact",  per_key(pre_index_bytes, N),     "bytes");
    m.add("index_bytes_per_key_post_compact", per_key(tb.index_bytes(), N),    "bytes");
    m.add("range_pre_compact",   ops_per_sec(r.range_ops, pre_range.secs),     "scans/s");
    m.add("range_post_compact",  ops_per_sec(r.range_ops, post_range.secs),    "scans/s");
}

// compact_lex() on a copy of the bulk table, with the same keys and ranges.
static void phase_compact_lex(Run& r) {
    const size_t N = r.N;
    std::cout << "\n";
    // Copy the original bulk files so compact_lex starts from the same state
    // as compact() did above.
    copy_table(r.data_dir + "/bulk", r.data_dir + "/bulklex");

    Table tb = Table::open(r.schema, r.data_dir + "/bulklex.trie", r.data_dir + "/bulklex.heap");

    auto tc0 = Clock::now();
    tb.compact_lex();
    double compact_lex_secs = Dur(Clock::now() - tc0).count();

    LookupStats lex       = measure_lookups(tb, r.lookup_keys);
    double      lex_blocks = blocks_per_lookup(tb, r.lookup_keys);
    RangeStats  lex_range  = measure_ranges(tb, r.bulk_ranges);

    std::cout << "COMPACT LEX\n"
              << "  time         : " << compact_lex_secs * 1000 << " ms\n"
              << "  alloc chains : " << tb.chain_count() << "\n"
              << "  index B/key  : " << per_key(tb.index_bytes(), N) << "\n\n";

    std::cout << "LOOKUP — post-compact-lex (bulk table, warm cache)\n"
              << "  throughput   : " << ops_per_sec(lex.found, lex.secs) / 1000 << " K ops/sec\n"
              << "  avg chains   : " << avg_chains(lex) << "\n"
              << "  avg blocks   : " << lex_blocks << "\n\n";

    std::cout << "RANGE  — post-compact-lex (" << r.range_ops << " scans, ~" << N/1000 << " keys each)\n"
              << "  rows returned: " << lex_range.rows << "\n"
              << "  throughput   : " << ops_per_sec(r.range_ops, lex_range.secs) << " scans/sec\n";

    Metrics& m = r.m;
    m.add("compact_lex_time",         compact_lex_secs * 1000,                 "ms");
    m.add("compact_lex_alloc_chains", double(tb.chain_count()),                "chains");
    m.add("lookup_post_compact_lex",  ops_per_sec(lex.found, lex.secs),        "ops/s");
    m.add("lookup_chains_post_compact_lex", avg_chains(lex),                   "chains");
    m.add("lookup_blocks_post_compact_lex", lex_blocks,                        "blocks");
    m.add("index_bytes_per_key_post_compact_lex", per_key(tb.index_bytes(), N), "bytes");
    m.add("range_post_compact_lex",   ops_per_sec(r.range_ops, lex_range.secs), "scans/s");
}

// ---- main ----

int main(int argc, char* argv[]) {
    // Usage: bench [N] [data_dir] [--csv file] [--keys random_u64|seq_u64|prefix_str|varlen_str]
    Run         r;
    std::string csv_path;
    for (int i = 1, pos = 0; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--csv" && i + 1 < argc)       csv_path   = argv[++i];
        else if (a == "--keys" && i + 1 < argc) r.key_dist = argv[++i];
        else if (pos++ == 0)                    r.N        = static_cast<size_t>(std::stoul(a));
        else                                    r.data_dir = a;
    }

    std::cout << "heavy-trie bench  N=" << r.N << "  keys=" << r.key_dist
              << "  data_dir=" << r.data_dir << "\n\n";

    // Wipe and recreate data directory for a fresh run.
    std::filesystem::remove_all(r.data_dir);
    std::filesystem::create_directories(r.data_dir);

    r.schema = make_schema(r.key_dist);
    r.keys   = make_keys(r.key_dist, r.N, r.rng);

    double bulk_secs = phase_bulk(r);
    // Stays open for the rest of the run, as the later phases always ran with it open.
    Table t = Table::create(r.schema, r.data_dir + "/bench.trie", r.data_dir + "/bench.heap");
    phase_single(r, t, bulk_secs);
    phase_churn(r, t);
    phase_compact(r);
    phase_compact_lex(r);

    // Cold start: the same lookups, starting from an evicted page cache.
    std::cout << "\n";
    cold_run("pre_compact",      r.schema, r.data_dir + "/bulkcold", r.lookup_keys, r.m);
    cold_run("post_compact",     r.schema, r.data_dir + "/bulk",     r.lookup_keys, r.m);
    cold_run("post_compact_lex", r.schema, r.data_dir + "/bulklex",  r.lookup_keys, r.m);

    r.m.write_csv(csv_path);
    return 0;
}
