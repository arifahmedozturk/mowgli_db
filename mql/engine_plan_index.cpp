#include "mql/engine.h"
#include <algorithm>

// Choosing a secondary index for a WHERE clause with no condition on the
// primary key (#97). An index serves a prefix of its columns: = (or IN) on
// each of its leading columns, then optionally bounds on the next one. The
// index whose usable prefix is longest wins; with none, the table is scanned.
// The rows an index scan reads still have every condition checked.

static constexpr size_t MAX_INDEX_RANGES = 256;   // IN lists multiply the scans

// The smallest and largest cells a column can hold.
static std::vector<uint8_t> lowest(const ColDef& col) {
    return std::vector<uint8_t>(col.type == ColType::UINT64 ? 8 : 0, 0);
}
static std::vector<uint8_t> highest(const ColDef& col) {
    return std::vector<uint8_t>(col.type == ColType::UINT64 ? 8 : col.max_size, 0xFF);
}

// For one index: the scans it would need, and how much of the clause they use.
static size_t index_ranges(const Schema& schema, const IndexDef& ix, const Where& where,
                           std::vector<std::pair<Row, Row>>& ranges) {
    ranges = {{Row{}, Row{}}};
    size_t score = 0;
    for (size_t col : ix.cols) {
        const ColDef& def = schema.cols[col];
        const Condition* eq = nullptr;
        const Condition* in = nullptr;
        std::vector<uint8_t> lo = lowest(def), hi = highest(def);
        bool bounded = false;
        for (const Condition& c : where) {
            if (Engine::column(schema, c.column) != col) continue;
            switch (c.op) {
            case Condition::Op::EQ: eq = &c; break;
            case Condition::Op::IN: in = &c; break;
            case Condition::Op::LT: case Condition::Op::LE:
                hi = std::min(hi, Engine::encode(c.value, def)); bounded = true; break;
            case Condition::Op::GT: case Condition::Op::GE:
                lo = std::max(lo, Engine::encode(c.value, def)); bounded = true; break;
            case Condition::Op::BETWEEN:
                lo = std::max(lo, Engine::encode(c.value, def));
                hi = std::min(hi, Engine::encode(c.upper, def));
                bounded = true;
                break;
            }
        }
        if (eq) {
            std::vector<uint8_t> v = Engine::encode(eq->value, def);
            for (auto& [l, h] : ranges) { l.push_back(v); h.push_back(v); }
            score += 2;
            continue;
        }
        if (in && ranges.size() * in->list.size() <= MAX_INDEX_RANGES) {
            // Each distinct value once, or a repeated one would read its rows twice.
            std::vector<std::vector<uint8_t>> values;
            for (const Literal& lit : in->list) values.push_back(Engine::encode(lit, def));
            std::sort(values.begin(), values.end());
            values.erase(std::unique(values.begin(), values.end()), values.end());
            std::vector<std::pair<Row, Row>> more;
            for (const auto& v : values)
                for (auto [l, h] : ranges) { l.push_back(v); h.push_back(v); more.push_back({l, h}); }
            ranges = std::move(more);
            score += 2;
            continue;
        }
        if (bounded) {
            for (auto& [l, h] : ranges) { l.push_back(lo); h.push_back(hi); }
            score += 1;
        }
        break;   // the index's later columns are not ordered within a range of this one
    }
    if (ranges.front().first.empty()) ranges.clear();
    return score;
}

std::optional<Engine::Plan> Engine::index_plan(const Schema& schema, const Where& where) {
    std::optional<Plan> best;
    size_t best_score = 0;
    for (const IndexDef& ix : schema.indexes) {
        std::vector<std::pair<Row, Row>> ranges;
        size_t score = index_ranges(schema, ix, where, ranges);
        if (score <= best_score || ranges.empty()) continue;
        best_score = score;
        Plan p;
        p.kind         = Plan::Kind::INDEX;
        p.index        = ix.name;
        p.index_ranges = std::move(ranges);
        best = std::move(p);
    }
    return best;
}
