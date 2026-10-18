#include "index/disk_trie.h"
#include "index/disk_trie_internal.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>
#include <shared_mutex>
#include <stdexcept>

// The hot chain cache, the per-chain key counts used for rebalancing, and
// flushing.

ChainCounts& DiskTrie::get_counts(uint64_t block_id) {
    return counts_[block_id];
}

void DiskTrie::init_counts(uint64_t block_id, uint32_t total, size_t num_nodes) {
    auto& cc = counts_[block_id];
    cc.total = total;
    cc.nodes.assign(num_nodes, NodeCounts{});
}

// ---- hot chain cache ----

ChainData DiskTrie::chain_read_shared(uint64_t block_id) const {
    {
        auto it = hot_.find(block_id);
        if (it != hot_.end())
            return it->second.data;
    }

    uint8_t slot = chain_addr_slot(block_id);
    if (slot == 0) {
        // Dedicated block: decode straight from the pinned frame.
        const uint8_t* frame = dm_.pin_chain_shared(block_id);
        ChainData data;
        chain_decode(frame, data);
        dm_.unpin_chain_shared(block_id);
        return data;
    }
    // Packed block (or forwarded): use read_chain_at which handles both.
    return dm_.read_chain_at(block_id);
}

ChainData DiskTrie::chain_read(uint64_t block_id) const {
    {
        auto it = hot_.find(block_id);
        if (it != hot_.end()) {
            cache_hits_.fetch_add(1, std::memory_order_relaxed);
            return it->second.data;
        }
    }
    cache_misses_.fetch_add(1, std::memory_order_relaxed);
    // Not cached: read from mmap (thread-safe, no lock held during I/O).
    ChainData data = dm_.read_chain(block_id);
    if (data.bucket) return data;   // buckets live only in the file (#73)
    {
        if (!hot_.count(block_id)) { // another reader may have inserted it concurrently
            if (hot_.size() >= HOT_CAPACITY) hot_evict_one();
            hot_insert(block_id, HotEntry{data, 0, hot_weight(block_id, &data)});
        }
    }
    return data;
}

// Move block_id's weight-map entry from hot_clean_ → hot_dirty_ (or vice versa).
// Called only when the dirty/clean state actually changes.
static void wmap_move(
        std::multimap<uint32_t,uint64_t>& from,
        std::multimap<uint32_t,uint64_t>& to,
        std::unordered_map<uint64_t, std::multimap<uint32_t,uint64_t>::iterator>& wpos,
        uint64_t block_id, uint32_t weight) {
    auto wit = wpos.find(block_id);
    if (wit == wpos.end()) return;
    from.erase(wit->second);
    wit->second = to.insert({weight, block_id});
}

void DiskTrie::chain_write(uint64_t block_id, ChainData chain) {
    if (chain.bucket) {
        // Buckets are never cached (#73): write through, replacing whatever
        // was cached at this address (a chain merged back into a bucket).
        hot_remove(block_id);
        dm_.update_chain(block_id, chain);
        return;
    }
    uint32_t new_weight = hot_weight(block_id, &chain);
    auto it = hot_.find(block_id);
    if (it != hot_.end()) {
        bool     was_dirty  = (it->second.dirty > 0);
        uint32_t old_weight = it->second.weight;

        // Update weight index if the subtree count changed.
        if (old_weight != new_weight) {
            auto& wmap = was_dirty ? hot_dirty_ : hot_clean_;
            auto wit = hot_wpos_.find(block_id);
            if (wit != hot_wpos_.end()) {
                wmap.erase(wit->second);
                wit->second = wmap.insert({new_weight, block_id});
            }
            it->second.weight = new_weight;
        }

        it->second.data = std::move(chain);
        it->second.dirty++;

        if (!was_dirty)  // just became dirty
            wmap_move(hot_clean_, hot_dirty_, hot_wpos_, block_id, new_weight);

        if (it->second.dirty >= DIRTY_FLUSH_AT) {
            dm_.update_chain(block_id, it->second.data);
            it->second.dirty = 0;
            wmap_move(hot_dirty_, hot_clean_, hot_wpos_, block_id, new_weight);
        }
    } else {
        if (hot_.size() >= HOT_CAPACITY)
            hot_evict_one();
        hot_insert(block_id, HotEntry{std::move(chain), 1, new_weight});
    }
}

uint64_t DiskTrie::chain_alloc(const ChainData& chain, uint64_t hint) {
    uint64_t hint_phys = (hint != NULL_BLOCK) ? chain_addr_phys(hint) : NULL_BLOCK;
    uint64_t id = dm_.alloc_chain_slot(chain, hint_phys);
    if (chain.bucket) return id;   // buckets live only in the file (#73)
    if (hot_.size() >= HOT_CAPACITY)
        hot_evict_one();
    // Weight is unknown until init_counts is called by the caller; use 1 for now.
    hot_insert(id, HotEntry{chain, 0, 1});
    return id;
}

uint32_t DiskTrie::hot_weight(uint64_t block_id, const ChainData* data) const {
    auto it = counts_.find(block_id);
    if (it != counts_.end() && it->second.total > 0) return it->second.total;
    if (data && data->bucket) return std::max<uint32_t>(1, static_cast<uint32_t>(bucket_size(*data)));
    // Counts not built yet (opened trie, no writes): estimate from the on-disk
    // floor(log2) child weights at the chain's first branch, so eviction still
    // favours heavy chains near the root instead of degrading to FIFO.
    if (data && !data->nodes.empty()) {
        const ChainNode& n = data->nodes.front();
        uint32_t w = 1u << std::min<uint16_t>(n.heavy_weight, 30);
        for (const auto& l : n.lights) w += 1u << std::min<uint16_t>(l.weight, 30);
        return w;
    }
    return 1;
}

void DiskTrie::hot_insert(uint64_t block_id, HotEntry entry) const {
    uint32_t w     = entry.weight;
    bool     dirty = entry.dirty > 0;
    hot_[block_id] = std::move(entry);
    auto& wmap = dirty ? hot_dirty_ : hot_clean_;
    hot_wpos_[block_id] = wmap.insert({w, block_id});
}

void DiskTrie::hot_remove(uint64_t block_id) const {
    auto wit = hot_wpos_.find(block_id);
    if (wit != hot_wpos_.end()) {
        auto it = hot_.find(block_id);
        bool dirty = (it != hot_.end() && it->second.dirty > 0);
        (dirty ? hot_dirty_ : hot_clean_).erase(wit->second);
        hot_wpos_.erase(wit);
    }
    hot_.erase(block_id);
}

void DiskTrie::hot_evict_one() const {
    // Always evict the lowest-weight clean entry in O(log n).
    // Only fall back to dirty if the clean map is empty.
    auto try_evict = [&](std::multimap<uint32_t, uint64_t>& wmap, bool flush) -> bool {
        while (!wmap.empty()) {
            auto wit    = wmap.begin();
            uint64_t v  = wit->second;
            wmap.erase(wit);
            hot_wpos_.erase(v);
            auto it = hot_.find(v);
            if (it == hot_.end()) continue; // stale
            if (flush && it->second.dirty > 0)
                dm_.update_chain(v, it->second.data);
            hot_.erase(it);
            return true;
        }
        return false;
    };

    if (!try_evict(hot_clean_, false)) // evict cheapest clean entry
        try_evict(hot_dirty_, true);   // fallback: flush + evict cheapest dirty
}

void DiskTrie::hot_invalidate(uint64_t block_id) {
    auto it = hot_.find(block_id);
    if (it == hot_.end()) return;
    if (it->second.dirty > 0)
        dm_.update_chain(block_id, it->second.data);
    hot_remove(block_id);
}

void DiskTrie::hot_discard(uint64_t block_id) {
    hot_remove(block_id);
}

void DiskTrie::hot_flush_all() {
    for (auto& [id, entry] : hot_)
        if (entry.dirty > 0)
            dm_.update_chain(id, entry.data);
    for (auto& [id, entry] : hot_)
        entry.dirty = 0;
    // Rebuild weight maps: all entries are now clean.
    hot_dirty_.clear();
    hot_clean_.clear();
    hot_wpos_.clear();
    for (auto& [id, entry] : hot_)
        hot_wpos_[id] = hot_clean_.insert({entry.weight, id});
}

// ---- per-chain counts ----

uint32_t DiskTrie::rebuild_chain_counts(uint64_t block_id) {
    ChainData chain = dm_.read_chain(block_id);
    if (chain.bucket) {
        auto& cc = counts_[block_id];
        cc.nodes.clear();
        cc.total = static_cast<uint32_t>(bucket_size(chain));
        return cc.total;
    }
    size_t num_nodes = chain.nodes.size();

    std::vector<NodeCounts> nodes(num_nodes);
    for (size_t ni = 0; ni < num_nodes; ni++)
        for (const auto& l : chain.nodes[ni].lights)
            nodes[ni].lights.push_back(rebuild_chain_counts(l.block));

    uint32_t suffix = chain.tail_record.valid() ? 1 : 0;
    for (int ni = (int)num_nodes - 1; ni >= 0; ni--) {
        // An end node has nothing past it on the chain (its tail key ends there).
        nodes[ni].heavy = chain.nodes[ni].split >= chain.path_len ? 0 : suffix;
        suffix += chain.nodes[ni].record.valid() ? 1 : 0;
        for (uint32_t c : nodes[ni].lights) suffix += c;
    }
    auto& cc = counts_[block_id];
    cc.nodes = std::move(nodes);
    cc.total = suffix;
    return suffix;
}

void DiskTrie::rebuild_counts() {
    rebuild_chain_counts(dm_.root_block());
}

void DiskTrie::ensure_counts() {
    if (counts_ready_) return;
    counts_.clear();
    if (dm_.root_block() != NULL_BLOCK) rebuild_counts();
    counts_ready_ = true;

    // Chains cached before now (e.g. by range scans) got weight 1; re-rank them
    // with the real subtree counts so eviction keeps the heavy chains.
    hot_clean_.clear();
    hot_dirty_.clear();
    hot_wpos_.clear();
    for (auto& [id, entry] : hot_) {
        entry.weight  = hot_weight(id, &entry.data);
        auto& wmap    = entry.dirty > 0 ? hot_dirty_ : hot_clean_;
        hot_wpos_[id] = wmap.insert({entry.weight, id});
    }
}

void DiskTrie::flush() {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    hot_flush_all();
}

