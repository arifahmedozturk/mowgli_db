#include "mql/engine.h"
#include "catalog/codec.h"
#include <shared_mutex>

// SELECT, and the admin statements: COMPACT, SHOW CHAINS, STATS, HELP.
// A SELECT prints one line per row, columns separated by " | ", then
// "(n rows)" (the server adds up these trailers when it merges a cluster's
// results); SELECT COUNT(*) prints the count.

std::string Engine::run_select(const Select& s) {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    Table& t = table(s.table);
    const Schema& schema = t.schema();
    std::vector<size_t> cols;
    for (const auto& name : s.columns) cols.push_back(column(schema, name));
    if (cols.empty())
        for (size_t c = 0; c < schema.cols.size(); c++) cols.push_back(c);

    const size_t limit = s.limit.value_or(SIZE_MAX);
    size_t n = 0;
    std::string out;
    if (limit > 0)
        scan(t, s.where, [&](const Row& row) {
            n++;
            if (!s.count) {
                for (size_t k = 0; k < cols.size(); k++) {
                    if (k > 0) out += " | ";
                    out += cell_to_text(schema.cols[cols[k]].type, row[cols[k]]);
                }
                out += "\n";
            }
            return n < limit;
        });
    if (s.count) return std::to_string(n);
    return out + "(" + std::to_string(n) + " rows)";
}

std::string Engine::run_compact(const Compact& s) {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    table(s.table).compact();
    return "OK";
}

std::string Engine::run_chains(const Chains& s) {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    const Table& t = table(s.table);
    return std::to_string(t.active_chain_count()) + " active, " + std::to_string(t.chain_count()) + " allocated";
}

std::string Engine::run_stats() {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    std::string out = "inflight: " + std::to_string(inflight.load(std::memory_order_relaxed));
    for (auto& [name, table] : tables_) {
        uint64_t hits = table.cache_hits(), total = hits + table.cache_misses();
        out += "\n" + name + ": " + std::to_string(table.record_count()) + " keys  cache " +
               std::to_string(hits) + "/" + std::to_string(total) + " (" +
               (total ? std::to_string(hits * 100 / total) + "%" : std::string("n/a")) + " hit)";
    }
    return out;
}

std::string Engine::help_text() {
    return
        "Statements (keywords in any case; an optional ';' at the end):\n"
        "  CREATE TABLE t (col type [PRIMARY KEY], ...)   types: NUMBER (INT, BIGINT),\n"
        "                                                  VARCHAR(n) (STRING, TEXT; 64 bytes by default)\n"
        "  DROP TABLE t        SHOW TABLES        DESCRIBE t\n"
        "  CREATE INDEX i ON t (col, ...)        DROP INDEX i ON t\n"
        "  INSERT INTO t VALUES (v, ...), (v, ...)        rows with a stored key are skipped\n"
        "  SELECT * | col, ... | COUNT(*) FROM t [WHERE cond AND ...] [LIMIT n]\n"
        "  UPDATE t SET col = v, ... [WHERE cond AND ...]\n"
        "  DELETE FROM t [WHERE cond AND ...]\n"
        "      cond: col = v, col < v, col <= v, col > v, col >= v, col BETWEEN v AND w,\n"
        "            col IN (v, ...); on the primary key these read only those keys or\n"
        "            that key range; else an index whose leading columns they constrain\n"
        "            is used; else every row is read\n"
        "  values: 123, 'text' ('' for a quote)\n"
        "  COMPACT t           repack the index for read locality\n"
        "  SHOW CHAINS t       active and allocated chain blocks\n"
        "  STATS               in-flight requests and per-table cache stats\n"
        "  HELP                this message\n"
        "  exit / quit         leave the REPL";
}
