#include "index/disk_trie.h"
#include "index/disk_trie_internal.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>
#include <shared_mutex>
#include <stdexcept>

// ---- compaction ----

void DiskTrie::compact_assign(uint64_t old_addr, uint64_t parent_new_phys,
                               std::unordered_map<uint64_t, uint64_t>& remap) {
    if (old_addr == NULL_BLOCK) return;

    ChainData chain   = dm_.read_chain_at(old_addr);
    uint64_t new_addr = dm_.alloc_chain_slot(chain, parent_new_phys);
    remap[old_addr]   = new_addr;

    uint64_t my_phys = chain_addr_phys(new_addr);
    for (auto& node : chain.nodes)
        for (auto& l : node.lights)
            compact_assign(l.block, my_phys, remap);
}

void DiskTrie::compact_apply(uint64_t old_root,
                              std::unordered_map<uint64_t, uint64_t>& remap) {
    // The new slots sit in blocks allocated while the old ones were live; once
    // the old ones are freed, compact_blocks slides those blocks down to the
    // front of the file (#53). Pointers are written with those final ids.
    std::vector<uint64_t> live;
    live.reserve(remap.size());
    for (auto& [old_addr, new_addr] : remap) live.push_back(chain_addr_phys(new_addr));
    std::sort(live.begin(), live.end());
    live.erase(std::unique(live.begin(), live.end()), live.end());
    std::unordered_map<uint64_t, uint64_t> final_phys;
    final_phys.reserve(live.size());
    for (size_t i = 0; i < live.size(); i++) final_phys[live[i]] = DiskManager::compacted_phys(i);
    auto final_addr = [&](uint64_t new_addr) {
        return make_chain_addr(final_phys.at(chain_addr_phys(new_addr)), chain_addr_slot(new_addr));
    };

    // Pass 2: rewrite each new slot with updated light-child pointers.
    // Remapping only changes fixed-width pointers, so every chain fits in place.
    for (auto& [old_addr, new_addr] : remap) {
        ChainData chain = dm_.read_chain_at(old_addr);
        for (auto& node : chain.nodes)
            for (auto& l : node.lights) l.block = final_addr(remap.at(l.block));
        uint64_t at = dm_.update_chain_at(new_addr, chain);
        assert(at == new_addr);
        (void)at;
    }
    std::vector<std::pair<uint64_t, uint64_t>> moves;   // old address -> final address
    moves.reserve(remap.size());
    for (auto& [old_addr, new_addr] : remap) moves.push_back({old_addr, final_addr(new_addr)});
    remap.clear();
    for (auto& [old_addr, fin] : moves) remap[old_addr] = fin;

    // Pass 3: re-key hot cache + counts_ from old→new addresses, fix up
    // light-child pointers inside cached ChainData, then free old slots.
    {
        std::unordered_map<uint64_t, HotEntry>  new_hot;
        std::multimap<uint32_t, uint64_t>        new_clean, new_dirty;
        std::unordered_map<uint64_t, std::multimap<uint32_t,uint64_t>::iterator> new_wpos;

        for (auto& [old_addr, new_addr] : remap) {
            auto it = hot_.find(old_addr);
            if (it != hot_.end()) {
                HotEntry& e = it->second;
                for (auto& node : e.data.nodes)
                    for (auto& l : node.lights) {
                        auto r = remap.find(l.block);
                        if (r != remap.end()) l.block = r->second;
                    }
                uint32_t w     = e.weight;
                bool     dirty = e.dirty > 0;
                new_hot[new_addr] = std::move(e);
                auto& wm = dirty ? new_dirty : new_clean;
                new_wpos[new_addr] = wm.insert({w, new_addr});
            }
        }
        hot_       = std::move(new_hot);
        hot_clean_ = std::move(new_clean);
        hot_dirty_ = std::move(new_dirty);
        hot_wpos_  = std::move(new_wpos);
    }

    {
        std::unordered_map<uint64_t, ChainCounts> new_counts;
        new_counts.reserve(remap.size());
        for (auto& [old_addr, new_addr] : remap) {
            auto it = counts_.find(old_addr);
            if (it != counts_.end())
                new_counts[new_addr] = std::move(it->second);
        }
        counts_ = std::move(new_counts);
    }

    for (auto& [old_addr, new_addr] : remap)
        dm_.free_chain_slot(old_addr);
    dm_.set_root_block(remap.at(old_root));
    dm_.compact_blocks(live);
}

void DiskTrie::compact() {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    ensure_counts();
    if (dm_.root_block() == NULL_BLOCK) return;

    hot_flush_all();
    dm_.clear_pack_candidates();

    uint64_t old_root = dm_.root_block();

    // Pass 1: DFS pre-order — each chain co-located with its direct light children.
    std::unordered_map<uint64_t, uint64_t> remap;
    remap.reserve(counts_.size());
    compact_assign(old_root, NULL_BLOCK, remap);

    compact_apply(old_root, remap);
}

void DiskTrie::compact_assign_lex(uint64_t old_addr, uint64_t& last_phys,
                                   std::unordered_map<uint64_t, uint64_t>& remap) {
    if (old_addr == NULL_BLOCK) return;

    ChainData chain = dm_.read_chain_at(old_addr);

    // Light subtrees whose digit is below the chain's own digit at that node
    // sort before the chain's continuation; place them first.
    auto before = [&](const ChainNode& node, const LightChild& l) {
        return node.split < chain.path_len && l.digit < chain.path[node.split];
    };
    for (auto& node : chain.nodes)
        for (auto& l : node.lights)
            if (before(node, l))
                compact_assign_lex(l.block, last_phys, remap);

    // Allocate this chain adjacent to its lex-predecessor.
    uint64_t new_addr = dm_.alloc_chain_slot(chain, last_phys);
    remap[old_addr]   = new_addr;
    last_phys         = chain_addr_phys(new_addr);

    for (auto& node : chain.nodes)
        for (auto& l : node.lights)
            if (!before(node, l))
                compact_assign_lex(l.block, last_phys, remap);
}

void DiskTrie::compact_lex() {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    ensure_counts();
    if (dm_.root_block() == NULL_BLOCK) return;

    hot_flush_all();
    dm_.clear_pack_candidates();

    uint64_t old_root = dm_.root_block();

    // Pass 1: in-order (lex) — chains allocated in lexicographic key order
    // so adjacent keys land in adjacent or the same packed block.
    std::unordered_map<uint64_t, uint64_t> remap;
    remap.reserve(counts_.size());
    uint64_t last_phys = NULL_BLOCK;
    compact_assign_lex(old_root, last_phys, remap);

    compact_apply(old_root, remap);
}

