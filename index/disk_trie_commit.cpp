#include "index/disk_trie.h"
#include <shared_mutex>

// Durability of committed operations (#28); see DiskTrie::commit in disk_trie.h.

void DiskTrie::free_slot(uint64_t addr) {
    if (durable_) freed_.push_back(addr);
    else          dm_.free_chain_slot(addr);
}

void DiskTrie::release_freed() {
    for (uint64_t addr : freed_) dm_.free_chain_slot(addr);
    freed_.clear();
}

// Only the dirty entries: hot_dirty_ lists them, so a commit costs nothing
// when the operation changed no cached chain.
void DiskTrie::flush_dirty() {
    while (!hot_dirty_.empty()) {
        auto wit = hot_dirty_.begin();
        uint64_t id = wit->second;
        uint32_t w  = wit->first;
        hot_dirty_.erase(wit);
        auto it = hot_.find(id);
        if (it == hot_.end()) { hot_wpos_.erase(id); continue; }   // stale
        dm_.update_chain(id, it->second.data);
        it->second.dirty = 0;
        hot_wpos_[id] = hot_clean_.insert({w, id});
    }
}

void DiskTrie::commit() {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    flush_dirty();      // parents now point at the slots they use...
    release_freed();    // ...so the slots they stopped using can go
    dm_.save_header();
}
