#include "mql/engine.h"
#include <shared_mutex>
#include <stdexcept>

// INSERT, UPDATE and DELETE. Each reports how many rows it changed, like
// SQL's command tags ("INSERT 3"). A row whose key is already stored is not
// inserted again (as with ON CONFLICT DO NOTHING); several rows go in as one
// bulk load. UPDATE and DELETE first collect the rows their WHERE matches,
// then change them: a scan holds the table's read latch.

static Row encode_row(const Schema& schema, const std::vector<Literal>& values) {
    if (values.size() != schema.cols.size())
        throw std::runtime_error("expected " + std::to_string(schema.cols.size()) + " values, got " +
                                 std::to_string(values.size()));
    Row row;
    for (size_t c = 0; c < values.size(); c++) row.push_back(Engine::encode(values[c], schema.cols[c]));
    return row;
}

std::string Engine::run_insert(const Insert& s) {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    Table& t = table(s.table);
    std::vector<Row> rows;
    for (const auto& values : s.rows) rows.push_back(encode_row(t.schema(), values));
    size_t n = rows.size() == 1 ? (t.insert(rows[0]) ? 1 : 0) : t.bulk_insert(std::move(rows));
    return "INSERT " + std::to_string(n);
}

std::string Engine::run_update(const Update& s) {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    Table& t = table(s.table);
    const Schema& schema = t.schema();
    std::vector<std::pair<size_t, std::vector<uint8_t>>> sets;
    for (const auto& [name, value] : s.set) {
        size_t col = column(schema, name);
        if (col == schema.pk_col) throw std::runtime_error("cannot UPDATE the primary key " + name);
        sets.push_back({col, encode(value, schema.cols[col])});
    }
    // Every column but the key set, on exact keys: the new rows are known
    // without reading the old ones.
    Plan p = plan(schema, s.where);
    const auto* keys = exact_keys(p);
    std::vector<bool> covered(schema.cols.size());
    for (const auto& set : sets) covered[set.first] = true;
    size_t n_covered = 0;
    for (bool c : covered) n_covered += c;
    if (keys && n_covered == schema.cols.size() - 1) {
        size_t n = 0;
        for (const auto& k : *keys) {
            Row r(schema.cols.size());
            r[schema.pk_col] = k;
            for (const auto& [col, bytes] : sets) r[col] = bytes;
            n += t.update(r);
        }
        return "UPDATE " + std::to_string(n);
    }
    std::vector<Row> rows;
    scan(t, s.where, [&](const Row& r) { rows.push_back(r); return true; });
    size_t n = 0;
    for (Row& r : rows) {
        for (const auto& [col, bytes] : sets) r[col] = bytes;
        n += t.update(r);
    }
    return "UPDATE " + std::to_string(n);
}

std::string Engine::run_delete(const Delete& s) {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    Table& t = table(s.table);
    const size_t pk = t.schema().pk_col;
    Plan p = plan(t.schema(), s.where);
    std::vector<std::vector<uint8_t>> keys;
    if (const auto* exact = exact_keys(p)) keys = *exact;   // no need to read the rows
    else scan(t, s.where, [&](const Row& r) { keys.push_back(r[pk]); return true; });
    size_t n = 0;
    for (const auto& k : keys) n += t.remove(k);
    return "DELETE " + std::to_string(n);
}
