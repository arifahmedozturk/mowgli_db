#include "mql/engine.h"
#include <shared_mutex>
#include <type_traits>

// Cluster routing (#92), read from the parsed statement: one key for an
// INSERT of one row or a WHERE that fixes the key; a key range for a SELECT
// whose WHERE bounds the key (the server merges the nodes' rows). Anything
// else, including a range UPDATE, DELETE or COUNT(*), runs where it arrives.

Engine::Route Engine::route(const std::string& cmd) const {
    Statement st = parse(cmd);
    Route r;
    r.write = is_write(st);
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    auto schema_of = [&](const std::string& name) -> const Schema* {
        auto it = tables_.find(name);
        return it == tables_.end() ? nullptr : &it->second.schema();
    };
    auto by_where = [&](const std::string& name, const Where& w, bool range_ok) {
        const Schema* s = schema_of(name);
        if (!s) return;
        Plan p = plan(*s, w);
        if (p.kind == Plan::Kind::KEY) {
            r.kind = Route::Kind::KEY;
            r.key.assign(p.key.begin(), p.key.end());
        } else if (p.kind == Plan::Kind::RANGE && range_ok) {
            r.kind = Route::Kind::RANGE;
            r.lo.assign(p.lo.begin(), p.lo.end());
            r.hi.assign(p.hi.begin(), p.hi.end());
        }
    };
    std::visit([&](const auto& s) {
        using T = std::decay_t<decltype(s)>;
        if constexpr (std::is_same_v<T, Insert>) {
            const Schema* sc = schema_of(s.table);
            if (sc && s.rows.size() == 1 && s.rows[0].size() == sc->cols.size()) {
                auto k = encode(s.rows[0][sc->pk_col], sc->cols[sc->pk_col]);
                r.kind = Route::Kind::KEY;
                r.key.assign(k.begin(), k.end());
            }
        } else if constexpr (std::is_same_v<T, Select>) {
            by_where(s.table, s.where, !s.count && !s.limit);
        } else if constexpr (std::is_same_v<T, Update> || std::is_same_v<T, Delete>) {
            by_where(s.table, s.where, false);
        }
    }, st);
    return r;
}
