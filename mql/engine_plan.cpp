#include "mql/engine.h"
#include "catalog/codec.h"
#include <algorithm>
#include <iterator>
#include <optional>
#include <stdexcept>

// Answering a WHERE clause. Conditions on the primary key narrow the rows
// read to one key or a key range; every condition is then checked on each
// row read, so a bound that is exclusive (<, >), or a condition on another
// column, just filters. = and IN on the key read only those keys; a clause
// with no condition on the key reads every row.

std::vector<uint8_t> Engine::encode(const Literal& v, const ColDef& col) {
    if (col.type == ColType::UINT64 && v.kind != Literal::Kind::NUMBER)
        throw std::runtime_error("expected a number for " + col.name + ", got '" + v.text + "'");
    if (col.type == ColType::VARCHAR && v.kind != Literal::Kind::STRING)
        throw std::runtime_error("expected a string for " + col.name + ", got " + v.text);
    return cell_from_text(col, v.text);
}

size_t Engine::column(const Schema& schema, const std::string& name) {
    for (size_t i = 0; i < schema.cols.size(); i++)
        if (schema.cols[i].name == name) return i;
    throw std::runtime_error("unknown column: " + name);
}

// An upper bound above every key of this column: longer than any key, all 0xFF.
static std::vector<uint8_t> above_all(const ColDef& col) {
    return std::vector<uint8_t>(std::max<size_t>(col.max_size, 8) + 1, 0xFF);
}

static std::vector<std::vector<uint8_t>> encode_set(const std::vector<Literal>& list, const ColDef& col) {
    std::vector<std::vector<uint8_t>> keys;
    for (const Literal& v : list) keys.push_back(Engine::encode(v, col));
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    return keys;
}

Engine::Plan Engine::plan(const Schema& schema, const Where& where) {
    using Keys = std::vector<std::vector<uint8_t>>;
    const ColDef& pk = schema.cols[schema.pk_col];
    std::optional<Keys> keys;   // the key is one of these (from = and IN), sorted
    std::vector<uint8_t> lo, hi;   // hi empty: no upper bound yet
    bool bounded = false, exact = true;
    auto lower_hi = [&](std::vector<uint8_t> v) { if (hi.empty() || v < hi) hi = std::move(v); };
    auto only = [&](Keys allowed) {
        if (!keys) { keys = std::move(allowed); return; }
        Keys both;
        std::set_intersection(keys->begin(), keys->end(), allowed.begin(), allowed.end(), std::back_inserter(both));
        keys = std::move(both);
    };
    for (const Condition& c : where) {
        if (column(schema, c.column) != schema.pk_col) { exact = false; continue; }
        if (c.op == Condition::Op::LT || c.op == Condition::Op::GT) exact = false;
        switch (c.op) {
        case Condition::Op::EQ: only({encode(c.value, pk)}); break;
        case Condition::Op::IN: only(encode_set(c.list, pk)); break;
        case Condition::Op::LT: case Condition::Op::LE:
            lower_hi(encode(c.value, pk)); bounded = true; break;
        case Condition::Op::GT: case Condition::Op::GE:
            lo = std::max(lo, encode(c.value, pk)); bounded = true; break;
        case Condition::Op::BETWEEN:
            lo = std::max(lo, encode(c.value, pk));
            lower_hi(encode(c.upper, pk));
            bounded = true;
            break;
        }
    }
    if (hi.empty()) hi = above_all(pk);
    Plan p;
    if (keys) {
        if (bounded)
            keys->erase(std::remove_if(keys->begin(), keys->end(),
                                       [&](const auto& k) { return k < lo || k > hi; }), keys->end());
        p.exact = exact;
        if (keys->empty()) p.kind = Plan::Kind::NONE;
        else if (keys->size() == 1) { p.kind = Plan::Kind::KEY; p.key = std::move(keys->front()); }
        else { p.kind = Plan::Kind::KEYS; p.keys = std::move(*keys); }
        return p;
    }
    if (lo > hi) { p.kind = Plan::Kind::NONE; return p; }
    p.kind = bounded ? Plan::Kind::RANGE : Plan::Kind::SCAN;
    p.lo   = std::move(lo);
    p.hi   = std::move(hi);
    return p;
}

// A condition with its column found and its values encoded, once per scan.
struct Filter {
    size_t                            col;
    Condition::Op                     op;
    std::vector<uint8_t>              v, upper;
    std::vector<std::vector<uint8_t>> set;   // IN, sorted
};

static std::vector<Filter> compile(const Schema& schema, const Where& where) {
    std::vector<Filter> fs;
    for (const Condition& c : where) {
        size_t col = Engine::column(schema, c.column);
        Filter f{col, c.op, {}, {}, {}};
        if (c.op == Condition::Op::IN) f.set = encode_set(c.list, schema.cols[col]);
        else f.v = Engine::encode(c.value, schema.cols[col]);
        if (c.op == Condition::Op::BETWEEN) f.upper = Engine::encode(c.upper, schema.cols[col]);
        fs.push_back(std::move(f));
    }
    return fs;
}

static bool matches(const std::vector<Filter>& fs, const Row& row) {
    for (const Filter& f : fs) {
        const std::vector<uint8_t>& cell = row[f.col];
        bool ok = false;
        switch (f.op) {
        case Condition::Op::EQ:      ok = cell == f.v; break;
        case Condition::Op::LT:      ok = cell <  f.v; break;
        case Condition::Op::LE:      ok = cell <= f.v; break;
        case Condition::Op::GT:      ok = cell >  f.v; break;
        case Condition::Op::GE:      ok = cell >= f.v; break;
        case Condition::Op::BETWEEN: ok = cell >= f.v && cell <= f.upper; break;
        case Condition::Op::IN:      ok = std::binary_search(f.set.begin(), f.set.end(), cell); break;
        }
        if (!ok) return false;
    }
    return true;
}

const std::vector<std::vector<uint8_t>>* Engine::exact_keys(Plan& p) {
    if (!p.exact) return nullptr;
    if (p.kind == Plan::Kind::KEY) { p.keys = {p.key}; p.kind = Plan::Kind::KEYS; }
    return p.kind == Plan::Kind::KEYS ? &p.keys : nullptr;
}

void Engine::scan(Table& t, const Where& where, const std::function<bool(const Row&)>& fn) {
    const Schema& schema = t.schema();
    Plan p = plan(schema, where);   // also rejects unknown columns and mistyped values
    // An exact plan's keys match without checking the rows.
    const std::vector<Filter> fs = p.exact ? std::vector<Filter>{} : compile(schema, where);
    switch (p.kind) {
    case Plan::Kind::NONE:
        return;
    case Plan::Kind::KEY: {
        Row row;
        if (t.lookup(p.key, &row) && matches(fs, row)) fn(row);
        return;
    }
    case Plan::Kind::KEYS:
        for (const auto& k : p.keys) {
            Row row;
            if (t.lookup(k, &row) && matches(fs, row) && !fn(row)) return;
        }
        return;
    case Plan::Kind::RANGE:
    case Plan::Kind::SCAN:
        t.scan_rows(p.lo, p.hi, [&](const RowView& v) {
            Row row = v.to_row();
            return !matches(fs, row) || fn(row);
        });
        return;
    }
}
