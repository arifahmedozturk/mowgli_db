#include "mql/engine.h"
#include <shared_mutex>
#include <type_traits>

// Before a mutation's WAL commit record is written, the table it wrote to
// writes its dirty chains to the mapped files and releases the slots the
// mutation freed (#28), so a committed command survives a crash of the
// process. Tables are durable from the moment the engine opens or creates
// them. Only the statement's own table is committed (#123): committing every
// table cost each write a latch and a header save per table, and committed
// other writers' half-done statements.
void Engine::commit_table(const Statement& s) {
    std::string name = std::visit([](const auto& st) -> std::string {
        if constexpr (requires { st.table; }) return st.table;
        else return {};
    }, s);
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    if (auto it = tables_.find(name); it != tables_.end()) it->second.commit();   // gone after DROP TABLE
}
