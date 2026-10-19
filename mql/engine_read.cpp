#include "mql/engine.h"
#include "catalog/codec.h"
#include <shared_mutex>

// SELECT, and the admin statements: COMPACT, SHOW CHAINS, STATS, HELP.
// A SELECT prints one line per row, columns separated by " | ", then
// "(n rows)" (the server adds up these trailers when it merges a cluster's
// results); SELECT COUNT(*) prints the count.

// Valid UTF-8 (by its byte structure) with no control characters: text
// that prints as itself.
static bool is_text(const std::vector<uint8_t>& s) {
    for (size_t i = 0; i < s.size();) {
        uint8_t b = s[i];
        if (b < 0x80) {
            if (b < 0x20 || b == 0x7F) return false;
            i++;
            continue;
        }
        size_t extra = (b >> 5) == 0x6 ? 1 : (b >> 4) == 0xE ? 2 : (b >> 3) == 0x1E ? 3 : 0;
        if (extra == 0 || i + extra >= s.size()) return false;   // not a lead byte, or cut short
        for (size_t k = 1; k <= extra; k++)
            if ((s[i + k] >> 6) != 0x2) return false;
        i += 1 + extra;
    }
    return true;
}

// A cell as SELECT prints it: a number in decimal; a string as itself, or
// as a hex literal x'...' unless it is text (is_text), so what is printed
// can be typed back (#82).
static std::string cell_text(ColType type, const std::vector<uint8_t>& cell) {
    if (type == ColType::UINT64) return cell_to_text(type, cell);
    if (is_text(cell)) return std::string(cell.begin(), cell.end());
    static const char* HEX = "0123456789abcdef";
    std::string out = "x'";
    for (uint8_t b : cell) { out += HEX[b >> 4]; out += HEX[b & 15]; }
    return out + "'";
}

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
                    out += cell_text(schema.cols[cols[k]].type, row[cols[k]]);
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
        "            col IN (v, ...), STARTS_WITH(col, 'prefix') (string columns);\n"
        "            on the primary key these read only those keys or\n"
        "            that key range; else an index whose leading columns they constrain\n"
        "            is used; else every row is read\n"
        "  values: 123, 'text' ('' for a quote), x'00ff' (any bytes; a string that is\n"
        "          not text, valid UTF-8 without control characters, prints so)\n"
        "  COMPACT t           repack the index for read locality\n"
        "  SHOW CHAINS t       active and allocated chain blocks\n"
        "  STATS               in-flight requests and per-table cache stats\n"
        "  HELP                this message\n"
        "  exit / quit         leave the REPL";
}
