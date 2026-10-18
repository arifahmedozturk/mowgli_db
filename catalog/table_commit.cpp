#include "catalog/table.h"

// Durability of committed operations (#28). Rows and heap headers live in the
// mapped heap file and need nothing more (#27); the tries hold dirty chains in
// their caches and defer frees until they commit.

void Table::set_durable(bool on) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    trie_->set_durable(on);
    for (auto& ix : indexes_) ix.trie->set_durable(on);
}

void Table::commit() {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    trie_->commit();
    for (auto& ix : indexes_) ix.trie->commit();
}
