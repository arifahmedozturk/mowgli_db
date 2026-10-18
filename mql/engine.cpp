#include "mql/engine.h"
#include "catalog/schema.h"
#include <filesystem>
#include <shared_mutex>
#include <stdexcept>
#include <type_traits>

Engine::Engine(const std::string& data_dir, bool wal, Wal::Sync sync)
    : data_dir_(data_dir)
    , wal_(Wal::open(data_dir + "/wal.log", sync))
    , wal_enabled_(wal)
{
    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
        if (entry.path().extension() != ".schema") continue;
        Schema schema = load_schema(entry.path().string());
        std::string name      = schema.table_name;
        std::string trie_path = data_dir_ + "/" + name + ".trie";
        std::string heap_path = data_dir_ + "/" + name + ".heap";
        if (std::filesystem::exists(trie_path) && std::filesystem::exists(heap_path))
            tables_.emplace(name, Table::open(schema, trie_path, heap_path)).first->second.set_durable(true);
    }

    // Replay any mutations that were written to the WAL but not committed
    // before the last crash. wal_on_ is false so replayed commands don't
    // get re-logged.
    auto pending = wal_.recover();
    if (!pending.empty()) {
        wal_on_ = false;
        for (const auto& cmd : pending) {
            try { exec(cmd); } catch (...) {}
        }
    }
    wal_on_ = wal_enabled_;
    wal_.truncate();
}

void Engine::compact_all() {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    for (auto& [name, tbl] : tables_)
        tbl.compact();
}

std::string Engine::exec(const std::string& cmd) {
    Statement s = parse(cmd);
    if (is_write(s)) return exec_mutation(cmd, s);
    return run(s);
}

std::string Engine::run(const Statement& s) {
    return std::visit([this](const auto& st) -> std::string {
        using T = std::decay_t<decltype(st)>;
        if constexpr (std::is_same_v<T, CreateTable>) return run_create(st);
        else if constexpr (std::is_same_v<T, DropTable>)  return run_drop(st);
        else if constexpr (std::is_same_v<T, ShowTables>) return run_show_tables();
        else if constexpr (std::is_same_v<T, Describe>)   return run_describe(st);
        else if constexpr (std::is_same_v<T, Insert>)     return run_insert(st);
        else if constexpr (std::is_same_v<T, Select>)     return run_select(st);
        else if constexpr (std::is_same_v<T, Update>)     return run_update(st);
        else if constexpr (std::is_same_v<T, Delete>)     return run_delete(st);
        else if constexpr (std::is_same_v<T, Compact>)    return run_compact(st);
        else if constexpr (std::is_same_v<T, Chains>)     return run_chains(st);
        else if constexpr (std::is_same_v<T, Stats>)      return run_stats();
        else                                              return help_text();
    }, s);
}

Table& Engine::table(const std::string& name) {
    auto it = tables_.find(name);
    if (it == tables_.end()) throw std::runtime_error("unknown table: " + name);
    return it->second;
}

const Table& Engine::table(const std::string& name) const {
    auto it = tables_.find(name);
    if (it == tables_.end()) throw std::runtime_error("unknown table: " + name);
    return it->second;
}
