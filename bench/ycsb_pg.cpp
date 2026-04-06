// YCSB workload harness for PostgreSQL.
// Uses the same generators and workload definitions as ycsb.cpp (via ycsb_common.h).
// Drives PostgreSQL through libpq with prepared statements.
//
// Setup (run once):
//   sudo service postgresql start
//   sudo -u postgres createuser --superuser $USER
//   createdb ycsb_bench
//
// Usage:
//   ./ycsb_pg [N] [--ops M] [A|B|C|D|E|F] [--connstr "..."] [--no-drop]

#include "bench/ycsb_common.h"
#include <libpq-fe.h>
#include <stdexcept>
#include <cstring>

// ---- PG helpers ----

static PGconn* pg_connect(const std::string& connstr) {
    PGconn* conn = PQconnectdb(connstr.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::string err = PQerrorMessage(conn);
        PQfinish(conn);
        throw std::runtime_error("PQconnectdb: " + err);
    }
    return conn;
}

static void pg_exec(PGconn* conn, const std::string& sql) {
    PGresult* r = PQexec(conn, sql.c_str());
    ExecStatusType s = PQresultStatus(r);
    if (s != PGRES_COMMAND_OK && s != PGRES_TUPLES_OK) {
        std::string err = PQresultErrorMessage(r);
        PQclear(r);
        throw std::runtime_error("pg_exec: " + err + "\nSQL: " + sql);
    }
    PQclear(r);
}

// Thin RAII wrapper around PGresult.
struct PGRes {
    PGresult* r;
    explicit PGRes(PGresult* r) : r(r) {}
    ~PGRes() { if (r) PQclear(r); }
    PGRes(const PGRes&) = delete;
    PGRes& operator=(const PGRes&) = delete;
};

// ---- schema helpers ----

// Build the CREATE TABLE statement: ycsb (ycsb_key CHAR(14) PRIMARY KEY, field0..field9 CHAR(100))
static std::string create_table_sql(const std::string& tname) {
    std::string s = "CREATE TABLE " + tname + " (ycsb_key CHAR(14) PRIMARY KEY";
    for (int i = 0; i < FIELD_COUNT; i++)
        s += ", field" + std::to_string(i) + " CHAR(" + std::to_string(FIELD_SIZE) + ")";
    s += ")";
    return s;
}

// Build parameterised INSERT for COPY-like bulk load (used by single-row inserts in op phase).
// We use a prepared statement: INSERT INTO tname VALUES ($1,$2,...,$11)
static void prepare_stmts(PGconn* conn, const std::string& tname) {
    // INSERT
    {
        std::string sql = "INSERT INTO " + tname + " VALUES ($1";
        for (int i = 2; i <= FIELD_COUNT + 1; i++)
            sql += ",$" + std::to_string(i);
        sql += ") ON CONFLICT DO NOTHING";
        PGRes r(PQprepare(conn, "ht_insert", sql.c_str(), 0, nullptr));
        if (PQresultStatus(r.r) != PGRES_COMMAND_OK)
            throw std::runtime_error(std::string("prepare insert: ") + PQresultErrorMessage(r.r));
    }
    // SELECT (point lookup)
    {
        std::string sql = "SELECT * FROM " + tname + " WHERE ycsb_key=$1";
        PGRes r(PQprepare(conn, "ht_read", sql.c_str(), 0, nullptr));
        if (PQresultStatus(r.r) != PGRES_COMMAND_OK)
            throw std::runtime_error(std::string("prepare read: ") + PQresultErrorMessage(r.r));
    }
    // UPDATE (one random field — we'll always update field0 for simplicity; same cost)
    {
        std::string sql = "UPDATE " + tname + " SET field0=$2 WHERE ycsb_key=$1";
        PGRes r(PQprepare(conn, "ht_update", sql.c_str(), 0, nullptr));
        if (PQresultStatus(r.r) != PGRES_COMMAND_OK)
            throw std::runtime_error(std::string("prepare update: ") + PQresultErrorMessage(r.r));
    }
    // SCAN (range with limit)
    {
        std::string sql = "SELECT * FROM " + tname
                        + " WHERE ycsb_key>=$1 ORDER BY ycsb_key LIMIT $2";
        PGRes r(PQprepare(conn, "ht_scan", sql.c_str(), 0, nullptr));
        if (PQresultStatus(r.r) != PGRES_COMMAND_OK)
            throw std::runtime_error(std::string("prepare scan: ") + PQresultErrorMessage(r.r));
    }
}

// ---- load phase (COPY for speed) ----

static constexpr size_t COPY_CHUNK = 1 << 20; // 1 MB per PQputCopyData call

static double load_phase(PGconn* conn, const std::string& tname,
                         uint64_t n, uint64_t seed) {
    std::mt19937_64 rng(seed);

    auto t0 = Clock::now();

    std::string copy_sql = "COPY " + tname + " FROM STDIN";
    PGRes res(PQexec(conn, copy_sql.c_str()));
    if (PQresultStatus(res.r) != PGRES_COPY_IN)
        throw std::runtime_error("COPY start failed: " + std::string(PQresultErrorMessage(res.r)));

    // Stream rows in 1 MB chunks to avoid a single giant allocation.
    // Each row: key\tfield0\t...\tfield9\n
    std::string buf;
    buf.reserve(COPY_CHUNK + 4096);
    for (uint64_t id = 0; id < n; id++) {
        buf += make_key(id);
        for (int f = 0; f < FIELD_COUNT; f++) {
            buf += '\t';
            buf += random_field_str(rng);
        }
        buf += '\n';
        if (buf.size() >= COPY_CHUNK) {
            if (PQputCopyData(conn, buf.c_str(), static_cast<int>(buf.size())) != 1)
                throw std::runtime_error("PQputCopyData failed");
            buf.clear();
        }
    }
    if (!buf.empty()) {
        if (PQputCopyData(conn, buf.c_str(), static_cast<int>(buf.size())) != 1)
            throw std::runtime_error("PQputCopyData failed");
    }
    if (PQputCopyEnd(conn, nullptr) != 1)
        throw std::runtime_error("PQputCopyEnd failed");

    PGRes end_res(PQgetResult(conn));
    if (PQresultStatus(end_res.r) != PGRES_COMMAND_OK)
        throw std::runtime_error("COPY end: " + std::string(PQresultErrorMessage(end_res.r)));

    return Dur(Clock::now() - t0).count();
}

// ---- operation phase ----

// Operations per explicit transaction. Large enough to amortise BEGIN/COMMIT
// overhead without hiding individual-operation latency in the averages.
static constexpr uint64_t TXN_BATCH = 100;

static WorkloadResult run_workload(Workload wl, PGconn* conn,
                                   uint64_t n_loaded, uint64_t n_ops,
                                   uint64_t seed) {
    const WorkloadDef& def = get_workload(wl);
    std::mt19937_64 rng(seed);

    uint64_t next_insert_id = n_loaded;
    ZipfianGenerator zipf(n_loaded);
    LatestGenerator  latest(n_loaded);
    std::uniform_real_distribution<double> pick(0.0, 1.0);
    std::uniform_int_distribution<int>     scan_len(1, MAX_SCAN_LEN);

    WorkloadResult res;

    pg_exec(conn, "BEGIN");
    for (uint64_t op = 0; op < n_ops; op++) {
        if (op > 0 && op % TXN_BATCH == 0) {
            pg_exec(conn, "COMMIT");
            pg_exec(conn, "BEGIN");
        }
        double r = pick(rng);

        if (r < def.read_p) {
            // READ
            uint64_t id = def.use_latest
                        ? latest.next(rng, next_insert_id)
                        : zipf.next_scrambled(rng);
            std::string key = make_key(id);
            const char* params[1] = {key.c_str()};
            auto t0 = Clock::now();
            PGRes qr(PQexecPrepared(conn, "ht_read", 1, params, nullptr, nullptr, 0));
            res.read.record(Dur(Clock::now() - t0).count());
            if (PQresultStatus(qr.r) != PGRES_TUPLES_OK)
                throw std::runtime_error("read: " + std::string(PQresultErrorMessage(qr.r)));

        } else if (r < def.read_p + def.update_p) {
            // UPDATE
            uint64_t id = zipf.next_scrambled(rng);
            std::string key   = make_key(id);
            std::string field = random_field_str(rng);
            const char* params[2] = {key.c_str(), field.c_str()};
            auto t0 = Clock::now();
            PGRes qr(PQexecPrepared(conn, "ht_update", 2, params, nullptr, nullptr, 0));
            res.update.record(Dur(Clock::now() - t0).count());
            if (PQresultStatus(qr.r) != PGRES_COMMAND_OK)
                throw std::runtime_error("update: " + std::string(PQresultErrorMessage(qr.r)));

        } else if (r < def.read_p + def.update_p + def.insert_p) {
            // INSERT
            uint64_t id = next_insert_id++;
            std::string key = make_key(id);
            std::vector<std::string> fields(FIELD_COUNT);
            std::vector<const char*> params(FIELD_COUNT + 1);
            params[0] = key.c_str();
            for (int f = 0; f < FIELD_COUNT; f++) {
                fields[f] = random_field_str(rng);
                params[f + 1] = fields[f].c_str();
            }
            auto t0 = Clock::now();
            PGRes qr(PQexecPrepared(conn, "ht_insert",
                                    FIELD_COUNT + 1, params.data(),
                                    nullptr, nullptr, 0));
            res.insert.record(Dur(Clock::now() - t0).count());
            if (PQresultStatus(qr.r) != PGRES_COMMAND_OK)
                throw std::runtime_error("insert: " + std::string(PQresultErrorMessage(qr.r)));

        } else if (r < def.read_p + def.update_p + def.insert_p + def.scan_p) {
            // SCAN
            uint64_t start = zipf.next_scrambled(rng);
            std::string start_key = make_key(start);
            std::string limit_str = std::to_string(scan_len(rng));
            const char* params[2] = {start_key.c_str(), limit_str.c_str()};
            auto t0 = Clock::now();
            PGRes qr(PQexecPrepared(conn, "ht_scan", 2, params, nullptr, nullptr, 0));
            res.scan.record(Dur(Clock::now() - t0).count());
            if (PQresultStatus(qr.r) != PGRES_TUPLES_OK)
                throw std::runtime_error("scan: " + std::string(PQresultErrorMessage(qr.r)));

        } else {
            // RMW: read then update, timed separately
            uint64_t id = zipf.next_scrambled(rng);
            std::string key = make_key(id);
            const char* rparams[1] = {key.c_str()};
            auto t0r = Clock::now();
            PGRes rr(PQexecPrepared(conn, "ht_read", 1, rparams, nullptr, nullptr, 0));
            res.read.record(Dur(Clock::now() - t0r).count());

            std::string field = random_field_str(rng);
            const char* wparams[2] = {key.c_str(), field.c_str()};
            auto t0w = Clock::now();
            PGRes wr(PQexecPrepared(conn, "ht_update", 2, wparams, nullptr, nullptr, 0));
            res.rmw.record(Dur(Clock::now() - t0w).count());
        }
    }
    pg_exec(conn, "COMMIT");

    return res;
}

// ---- per-workload runner ----

static void run_and_report(Workload wl, const std::string& connstr,
                           uint64_t n, uint64_t ops, bool drop_after,
                           bool async_commit) {
    const WorkloadDef& def = get_workload(wl);
    std::cout << "\n=== Workload " << def.label << " ===\n";

    PGconn* conn = pg_connect(connstr);
    pg_exec(conn, "SET client_min_messages = warning");
    if (async_commit)
        pg_exec(conn, "SET synchronous_commit = off");
    std::string tname = std::string("ycsb_") + def.label;

    pg_exec(conn, "DROP TABLE IF EXISTS " + tname);
    pg_exec(conn, create_table_sql(tname));
    prepare_stmts(conn, tname);

    std::cout << "  [load]  " << n << " records... " << std::flush;
    double ls = load_phase(conn, tname, n, 42);
    // Analyse so planner has fresh stats.
    pg_exec(conn, "ANALYZE " + tname);
    std::cout << std::fixed << std::setprecision(2)
              << ls * 1000 << " ms  (" << n / ls / 1000 << " K/s)\n";

    std::cout << "  [run]   " << ops << " operations\n";
    WorkloadResult res = run_workload(wl, conn, n, ops, 123);
    print_result(def.label, res);

    if (drop_after)
        pg_exec(conn, "DROP TABLE IF EXISTS " + tname);
    PQfinish(conn);
}

// ---- main ----

int main(int argc, char* argv[]) {
    uint64_t    N         = 100'000;
    uint64_t    OPS       = 100'000;
    std::string filter    = "ALL";
    std::string connstr   = "dbname=ycsb_bench";
    bool        drop      = true;
    bool        async     = false;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if      (a == "--ops"     && i+1 < argc) OPS     = std::stoull(argv[++i]);
        else if (a == "--connstr" && i+1 < argc) connstr = argv[++i];
        else if (a == "--no-drop")               drop    = false;
        else if (a == "--async")                 async   = true;
        else if (a.size() == 1 && a[0] >= 'A' && a[0] <= 'F') filter = a;
        else if (std::isdigit(static_cast<unsigned char>(a[0]))) N = std::stoull(a);
    }

    std::cout << "PostgreSQL YCSB  N=" << N << "  ops=" << OPS
              << "  txn_batch=" << TXN_BATCH
              << "  sync=" << (async ? "off" : "on")
              << "  connstr=\"" << connstr << "\"\n";
    std::cout << "Record: " << FIELD_COUNT << " fields x " << FIELD_SIZE
              << " B  key=14B\n";

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
        run_and_report(wl, connstr, N, OPS, drop, async);
    }

    return 0;
}
