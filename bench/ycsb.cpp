// YCSB workload harness for heavy-trie.
// See bench/ycsb_common.h for workload definitions and shared types.
//
// Usage:
//   ./ycsb [N] [--ops M] [A|B|C|D|E|F] [--data dir]

#include "bench/ycsb_common.h"
#include "catalog/table.h"
#include <filesystem>

// ---- record helpers ----

static std::vector<uint8_t> encode_key(uint64_t n) {
    std::string k = make_key(n);
    return {k.begin(), k.end()};
}

static Schema make_schema(const std::string& name) {
    Schema s;
    s.table_name = name;
    s.pk_col     = 0;
    s.cols.push_back({"key", ColType::VARCHAR, 14});
    for (int i = 0; i < FIELD_COUNT; i++)
        s.cols.push_back({"field" + std::to_string(i), ColType::VARCHAR,
                          static_cast<uint16_t>(FIELD_SIZE)});
    return s;
}

static Row make_row(uint64_t key_id, std::mt19937_64& rng) {
    Row row;
    row.push_back(encode_key(key_id));
    for (int i = 0; i < FIELD_COUNT; i++) {
        std::string f = random_field_str(rng);
        row.push_back({f.begin(), f.end()});
    }
    return row;
}

// ---- load phase ----

static double load_phase(Table& t, uint64_t n, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::vector<Row> rows;
    rows.reserve(n);
    for (uint64_t id = 0; id < n; id++)
        rows.push_back(make_row(id, rng));
    auto t0 = Clock::now();
    t.bulk_insert(std::move(rows));
    return Dur(Clock::now() - t0).count();
}

// ---- operation phase ----

static WorkloadResult run_workload(Workload wl, Table& t,
                                   uint64_t n_loaded, uint64_t n_ops,
                                   uint64_t seed) {
    const WorkloadDef& def = get_workload(wl);
    std::mt19937_64 rng(seed);

    uint64_t next_insert_id = n_loaded;
    ZipfianGenerator zipf(n_loaded);
    LatestGenerator  latest(n_loaded);
    std::uniform_real_distribution<double> pick(0.0, 1.0);
    std::uniform_int_distribution<int>     scan_len(1, MAX_SCAN_LEN);
    std::uniform_int_distribution<int>     field_pick(1, FIELD_COUNT);

    WorkloadResult res;

    for (uint64_t op = 0; op < n_ops; op++) {
        double r = pick(rng);

        if (r < def.read_p) {
            uint64_t id = def.use_latest
                        ? latest.next(rng, next_insert_id)
                        : zipf.next_scrambled(rng);
            auto t0 = Clock::now();
            Row row;
            t.lookup(encode_key(id), &row);
            res.read.record(Dur(Clock::now() - t0).count());

        } else if (r < def.read_p + def.update_p) {
            uint64_t id = zipf.next_scrambled(rng);
            Row row;
            if (t.lookup(encode_key(id), &row) && !row.empty()) {
                std::string f = random_field_str(rng);
                row[field_pick(rng)] = {f.begin(), f.end()};
                auto t0 = Clock::now();
                t.update(row);
                res.update.record(Dur(Clock::now() - t0).count());
            }

        } else if (r < def.read_p + def.update_p + def.insert_p) {
            uint64_t id = next_insert_id++;
            auto t0 = Clock::now();
            t.insert(make_row(id, rng));
            res.insert.record(Dur(Clock::now() - t0).count());

        } else if (r < def.read_p + def.update_p + def.insert_p + def.scan_p) {
            uint64_t start = zipf.next_scrambled(rng);
            uint64_t end   = start + static_cast<uint64_t>(scan_len(rng));
            auto t0 = Clock::now();
            auto rows = t.range(encode_key(start), encode_key(end));
            res.scan.record(Dur(Clock::now() - t0).count());
            (void)rows;

        } else {
            // RMW: read + update, timed separately
            uint64_t id = zipf.next_scrambled(rng);
            auto t0r = Clock::now();
            Row row;
            bool found = t.lookup(encode_key(id), &row);
            res.read.record(Dur(Clock::now() - t0r).count());
            if (found && !row.empty()) {
                std::string f = random_field_str(rng);
                row[field_pick(rng)] = {f.begin(), f.end()};
                auto t0w = Clock::now();
                t.update(row);
                res.rmw.record(Dur(Clock::now() - t0w).count());
            }
        }
    }

    return res;
}

// ---- per-workload runner ----

static void run_and_report(Workload wl, const std::string& data_dir,
                           uint64_t n, uint64_t ops) {
    const WorkloadDef& def = get_workload(wl);
    std::cout << "\n=== Workload " << def.label << " ===\n";

    Schema s   = make_schema(std::string("usertable_") + def.label);
    std::string tp = data_dir + "/ycsb_" + def.label + ".trie";
    std::string hp = data_dir + "/ycsb_" + def.label + ".heap";
    Table t = Table::create(s, tp, hp);

    std::cout << "  [load]  " << n << " records... " << std::flush;
    double ls = load_phase(t, n, 42);
    std::cout << std::fixed << std::setprecision(2)
              << ls * 1000 << " ms  (" << n / ls / 1000 << " K/s)\n";

    std::cout << "  [run]   " << ops << " operations\n";
    WorkloadResult res = run_workload(wl, t, n, ops, 123);
    print_result(def.label, res);
}

// ---- main ----

int main(int argc, char* argv[]) {
    uint64_t    N        = 100'000;
    uint64_t    OPS      = 100'000;
    std::string filter   = "ALL";
    std::string data_dir = "./ycsb_data";

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if      (a == "--ops"  && i+1 < argc) OPS      = std::stoull(argv[++i]);
        else if (a == "--data" && i+1 < argc) data_dir = argv[++i];
        else if (a.size() == 1 && a[0] >= 'A' && a[0] <= 'F') filter = a;
        else if (std::isdigit(static_cast<unsigned char>(a[0]))) N = std::stoull(a);
        else data_dir = a;
    }

    std::cout << "heavy-trie YCSB  N=" << N << "  ops=" << OPS
              << "  data=" << data_dir << "\n";
    std::cout << "Record: " << FIELD_COUNT << " fields x " << FIELD_SIZE
              << " B  key=14B\n";

    std::filesystem::remove_all(data_dir);
    std::filesystem::create_directories(data_dir);

    std::cout << "Building Zipfian table... " << std::flush;
    auto tz0 = Clock::now();
    ZipfianGenerator zipf_warmup(N); (void)zipf_warmup;
    std::cout << std::fixed << std::setprecision(1)
              << Dur(Clock::now() - tz0).count() * 1000 << " ms\n";

    Workload all[] = {Workload::A, Workload::B, Workload::C,
                      Workload::D, Workload::E, Workload::F};
    for (Workload wl : all) {
        const char* lbl = get_workload(wl).label;
        if (filter != "ALL" && filter != lbl) continue;
        run_and_report(wl, data_dir, N, OPS);
    }

    return 0;
}
