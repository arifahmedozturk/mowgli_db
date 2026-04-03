#pragma once
#include "storage/disk_manager.h"
#include "index/chain.h"
#include <cstddef>
#include <cstdint>
#include <list>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

struct NodeCounts {
    uint32_t light = 0;
    uint32_t heavy = 0;
};

struct ChainCounts {
    uint32_t total = 0;
    std::vector<NodeCounts> nodes; // parallel to ChainData.nodes[]
};

class DiskTrie {
public:
    explicit DiskTrie(DiskManager& dm) : dm_(dm) {
        if (dm_.root_block() != NULL_BLOCK)
            rebuild_counts();
    }

    ~DiskTrie() { hot_flush_all(); }

    // insert returns the number of flips triggered. Returns 0 on duplicate key.
    size_t insert(const std::string& key, RecordPtr record);
    // remove returns true if the key existed and was deleted.
    bool   remove(const std::string& key);
    // lookup returns false if not found. ptr_out receives the record pointer if found.
    bool   lookup(const std::string& key,
                  RecordPtr* ptr_out    = nullptr,
                  size_t*    chains_out = nullptr) const;

    size_t total_flips() const { return total_flips_; }
    size_t active_chain_count() const;

    // Bulk insert: inserts all (key, record) pairs without rebalancing, then
    // rebuilds counts in a single DFS pass. Much faster than N individual inserts
    // when keys are already sorted. Skips duplicates silently.
    void bulk_insert(std::vector<std::pair<std::string, RecordPtr>> kvs);

    // Collect all (key, RecordPtr) pairs with lo <= key <= hi, in lex order.
    void range_scan(const std::string& lo, const std::string& hi,
                    std::vector<std::pair<std::string, RecordPtr>>& out) const;

    // Read chain from hot cache if present, else load from disk and cache.
    // Also used by static range-scan traversal helpers.
    ChainData chain_read(uint64_t block_id) const;

    // Read-only cache check safe for concurrent readers under shared trie_latch_.
    // If the block is in the hot cache (possibly dirty), returns cached data.
    // If not, decodes directly from mmap (guaranteed authoritative for non-cached blocks).
    // Does NOT update LRU — no mutation of hot_ state.
    ChainData chain_read_shared(uint64_t block_id) const;

    // Direct mmap pin/unpin for range-scan traversal.
    const uint8_t* dm_pin_shared  (uint64_t block_id) const { return dm_.pin_chain_shared(block_id); }
    void           dm_unpin_shared(uint64_t block_id) const { dm_.unpin_chain_shared(block_id); }

private:
    DiskManager& dm_;
    std::unordered_map<uint64_t, ChainCounts> counts_;
    size_t total_flips_ = 0;
    mutable std::shared_mutex trie_latch_; // shared=read, exclusive=write

    // ---- hot chain cache ----
    // Holds decoded ChainData for the most-accessed blocks.
    // All methods must be called under trie_latch_ (any mode for reads, exclusive for writes).
    static constexpr size_t   HOT_CAPACITY   = 1000;
    static constexpr uint32_t DIRTY_FLUSH_AT = 10;

    struct HotEntry {
        ChainData data;
        uint32_t  dirty = 0; // unflushed update count
    };

    mutable std::unordered_map<uint64_t, HotEntry>                      hot_;
    mutable std::list<uint64_t>                                          hot_lru_;
    mutable std::unordered_map<uint64_t, std::list<uint64_t>::iterator>  hot_lru_pos_;

    // Write chain: updates hot cache, increments dirty; flushes to disk if dirty >= DIRTY_FLUSH_AT.
    void      chain_write(uint64_t block_id, ChainData chain);
    // Allocate a new block, write chain to disk immediately, add to hot cache clean.
    uint64_t  chain_alloc(const ChainData& chain);

    void hot_touch     (uint64_t block_id) const;
    void hot_evict_one () const; // evict LRU, flush to disk if dirty
    void hot_invalidate(uint64_t block_id); // remove (flush if dirty)
    void hot_discard   (uint64_t block_id); // remove WITHOUT flushing (for freed chains)
    void hot_flush_all ();                  // flush all dirty entries (destructor / close)

    ChainCounts&   get_counts(uint64_t block_id);
    void           init_counts(uint64_t block_id, uint32_t total, size_t num_nodes);
    static uint16_t floor_log2(uint32_t n);

    // Rebuild counts_ from disk after opening an existing trie file.
    void     rebuild_counts();
    uint32_t rebuild_chain_counts(uint64_t block_id);

    // Insert one key without updating weights or triggering flips.
    // Used by bulk_insert. Caller holds trie_latch_ exclusively.
    static bool insert_one_no_rebalance(DiskTrie& trie, DiskManager& dm,
                                        const std::string& key, RecordPtr record);

    // Flip heavy/light at nodes[ni] in the chain at block_id.
    void flip(uint64_t block_id, ChainData& chain, size_t ni);

    struct Frame { uint64_t block_id; size_t ni; bool went_light; };

    // Walk stack from deepest to root, incrementing (insert) or decrementing
    // (remove) the relevant subtree count and rewriting weights on disk when
    // floor_log2 changes.  Triggers flip when the adjusted side causes imbalance.
    void adjust_weights(const std::vector<Frame>& stack, bool increment);
};
