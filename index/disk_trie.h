#pragma once
#include "storage/disk_manager.h"
#include "index/chain.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

// Cursor for ordered iteration over the trie.
// Obtain via DiskTrie::lower_bound(); advance via DiskTrie::cursor_next().
struct TrieCursor {
    struct Frame {
        uint64_t          block_id;
        size_t            cbit_start; // chain bit offset to resume from (0 = chain start)
        std::vector<bool> prefix;     // all key bits accumulated before cbit_start
    };
    std::vector<Frame> stack;         // LIFO; stack.back() = next subtree to visit
    bool        has_pending = false;  // first result buffered by lower_bound
    std::string pending_key;
    RecordPtr   pending_rec{NULL_BLOCK, 0};
};

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
    size_t   active_chain_count() const;
    uint64_t cache_hits()   const { return cache_hits_.load(std::memory_order_relaxed); }
    uint64_t cache_misses() const { return cache_misses_.load(std::memory_order_relaxed); }

    // Bulk insert: inserts all (key, record) pairs without rebalancing, then
    // rebuilds counts in a single DFS pass. Much faster than N individual inserts
    // when keys are already sorted. Skips duplicates silently.
    void bulk_insert(std::vector<std::pair<std::string, RecordPtr>> kvs);

    // Compact: rewrite all chains on disk in DFS pre-order, co-locating each
    // chain with its direct light children in the same packed block.
    // Frees all old slots and updates the root pointer.
    void compact();

    // Compact: same as compact() but lays chains out in lexicographic key order.
    // Adjacent-in-key-order chains land in the same or adjacent packed blocks,
    // improving sequential range-scan I/O at the cost of point-lookup locality.
    void compact_lex();

    // Collect all (key, RecordPtr) pairs with lo <= key <= hi, in lex order.
    void range_scan(const std::string& lo, const std::string& hi,
                    std::vector<std::pair<std::string, RecordPtr>>& out) const;

    // Position cursor at the first key >= lo.  Returns false if no such key exists.
    // Use cursor_next() to advance; cursor_next() also emits the first key.
    bool lower_bound(const std::string& lo, TrieCursor& cursor) const;

    // Advance cursor to the next key in ascending order.
    // Returns true and populates key/rec on success; returns false when exhausted.
    bool cursor_next(TrieCursor& cursor, std::string& key, RecordPtr& rec) const;

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
    mutable std::shared_mutex trie_latch_; // shared=read/range, exclusive=write
    mutable std::mutex        hot_mu_;     // guards hot_ maps; always acquired after trie_latch_
    mutable std::atomic<uint64_t> cache_hits_{0};
    mutable std::atomic<uint64_t> cache_misses_{0};

    // ---- hot chain cache ----
    // Holds decoded ChainData for the most-accessed blocks.
    // Write paths access hot_ under exclusive trie_latch_ (no hot_mu_ needed).
    // Read paths (range scan, lookup) access hot_ under shared trie_latch_ + hot_mu_.
    static constexpr size_t   HOT_CAPACITY   = 13'500; // ~5 MB of decoded ChainData
    static constexpr uint32_t DIRTY_FLUSH_AT = 10;

    struct HotEntry {
        ChainData data;
        uint32_t  dirty  = 0; // unflushed update count
        uint32_t  weight = 1; // subtree key count — drives eviction priority
    };

    // Weight-based eviction: chains covering fewer keys are evicted first.
    // Two separate weight maps — one for clean entries (dirty==0) and one for
    // dirty entries — so hot_evict_one can find the cheapest clean victim in
    // O(log n) without scanning for a non-dirty entry.
    mutable std::unordered_map<uint64_t, HotEntry>                                        hot_;
    mutable std::multimap<uint32_t, uint64_t>                                             hot_clean_;  // weight→block_id, dirty==0
    mutable std::multimap<uint32_t, uint64_t>                                             hot_dirty_;  // weight→block_id, dirty>0
    mutable std::unordered_map<uint64_t, std::multimap<uint32_t,uint64_t>::iterator>      hot_wpos_;   // block_id → iterator into clean or dirty map

    // Write chain: updates hot cache, increments dirty; flushes to disk if dirty >= DIRTY_FLUSH_AT.
    void      chain_write(uint64_t block_id, ChainData chain);
    // Allocate a new block, write chain to disk immediately, add to hot cache clean.
    uint64_t  chain_alloc(const ChainData& chain, uint64_t hint = NULL_BLOCK);

    void hot_evict_one () const; // evict lowest-weight clean entry (or dirty if all dirty)
    void hot_insert    (uint64_t block_id, HotEntry entry) const; // insert + index
    void hot_remove    (uint64_t block_id) const;                 // remove from hot_ and weight maps
    void hot_invalidate(uint64_t block_id); // remove (flush if dirty)
    void hot_discard   (uint64_t block_id); // remove WITHOUT flushing (for freed chains)
    void hot_flush_all ();                  // flush all dirty entries (destructor / close)

    // Bloom filter for fast negative-lookup short-circuit.
    // 512 KB = 4M bits → false positive rate ~0.1% at 500k keys.
    static constexpr size_t BLOOM_BITS  = 4'194'304; // 2^22
    static constexpr size_t BLOOM_BYTES = BLOOM_BITS / 8;
    static constexpr int    BLOOM_K     = 7;          // number of hash functions
    std::vector<uint8_t> bloom_;   // allocated on first insert

    void   bloom_add (const std::string& key);
    bool   bloom_may_contain(const std::string& key) const; // false = definitely absent

    // Returns the current subtree key count for block_id, or 1 if unknown.
    uint32_t hot_weight(uint64_t block_id) const;

    ChainCounts&   get_counts(uint64_t block_id);
    void           init_counts(uint64_t block_id, uint32_t total, size_t num_nodes);
    static uint16_t floor_log2(uint32_t n);

    // Rebuild counts_ from disk after opening an existing trie file.
    void     rebuild_counts();
    uint32_t rebuild_chain_counts(uint64_t block_id);

    // DFS pre-order pass for compact(): allocates new slots and builds old→new remap.
    void compact_assign(uint64_t old_addr, uint64_t parent_new_phys,
                        std::unordered_map<uint64_t, uint64_t>& remap);

    // In-order (lex) pass for compact_lex(): allocates chains in lexicographic order.
    // last_phys is threaded through so each chain is packed near its lex-neighbor.
    void compact_assign_lex(uint64_t old_addr, uint64_t& last_phys,
                            std::unordered_map<uint64_t, uint64_t>& remap);

    // Shared passes 2+3 for both compact variants: rewrite pointers, re-key cache,
    // free old slots, and update the root pointer.  Caller holds trie_latch_ exclusively.
    void compact_apply(uint64_t old_root,
                       std::unordered_map<uint64_t, uint64_t>& remap);

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
