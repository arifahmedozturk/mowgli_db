#include "mql/engine.h"
#include <shared_mutex>

// Before a mutation's WAL commit record is written, every table writes its
// dirty chains to the mapped files and releases the slots the mutation freed
// (#28), so a committed command survives a crash of the process. Tables are
// durable from the moment the engine opens or creates them.
void Engine::commit_tables() {
    std::shared_lock<std::shared_mutex> lock(tables_latch_);
    for (auto& [name, table] : tables_) table.commit();
}
