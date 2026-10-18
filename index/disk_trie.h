#pragma once
#include <cstdio>
#include "storage/disk_manager.h"
#include "index/chain.h"
#include "index/bloom.h"
#include <atomic>
#include <functional>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

// Cursor for ordered iteration over the trie.
// Obtain via DiskTrie::lower_bound(); advance via DiskTrie::cursor_next().
struct TrieCursor {
    struct Frame {
        uint64_t             block_id;
        size_t               cpos_start; // chain nibble to resume from (0 = chain start);
                                         // for a bucket, the entry index to resume from
        std::vector<uint8_t> prefix;     // key nibbles accumulated before cpos_start
        std::shared_ptr<const ChainData> cached; // bucket already read (avoids re-reading per key)
        // For a bucket frame, set on first use: the key bytes before the bucket
        // and a pending high nibble if it starts mid-byte (#75).
        std::string head;
        int         odd     = -1;
        bool        head_ok = false;
    };
    std::vector<Frame> stack;         // LIFO; stack.back() = next subtree to visit
    bool        has_pending = false;  // first result buffered by lower_bound
    std::string pending_key;
    RecordPtr   pending_rec{NULL_BLOCK, 0};
};

struct NodeCounts {
    uint32_t              heavy = 0;  // keys continuing past this node on the chain
    std::vector<uint32_t> lights;     // keys under each light child, parallel to ChainNode::lights
};

struct ChainCounts {
    uint32_t total = 0;
    std::vector<NodeCounts> nodes; // parallel to ChainData.nodes[]
};

// Subtree buckets (#13): a subtree whose keys fit in bucket_max_bytes is stored
// as one sorted block. A bucket that outgrows it bursts into a chain with
// bucket children; a chain subtree that shrinks to bucket_merge_keys collapses
// back into a bucket.
struct TrieOptions {
    size_t   bucket_max_bytes  = BLOCK_SIZE;
    uint32_t bucket_merge_keys = 64;
};

class DiskTrie {
public:
    // bloom_path: optional sidecar file for the Bloom filter. It is loaded on
    // open (instead of rebuilding from every key) and deleted straight away,
    // then rewritten on clean close, so a crash leaves no stale filter behind.
    explicit DiskTrie(DiskManager& dm, std::string bloom_path = "", TrieOptions opts = {})
        : dm_(dm), opts_(opts), bloom_path_(std::move(bloom_path)) {
        if (dm_.root_block() != NULL_BLOCK) {
            counts_ready_ = false;  // rebuilt on the first write (ensure_counts)
            bloom_loaded_ = load_bloom();
            if (!bloom_loaded_) rebuild_bloom();
        }
        if (!bloom_path_.empty()) std::remove(bloom_path_.c_str());
    }

    ~DiskTrie() { hot_flush_all(); release_freed(); save_bloom(); }

    // ---- durability (#28, disk_trie_commit.cpp) ----
    // With durable on, slots freed by an operation stay intact in the file
    // until commit(), so a crash mid-operation leaves the old structure whole
    // (new slots it wrote are leaked at worst). commit() writes every dirty
    // cached chain to the mapped file, then releases the freed slots and saves
    // the file header (root, key count, free list): after it returns, the
    // operation's effect survives a crash of the process.
    void set_durable(bool on) { durable_ = on; }
    void commit();

    // True if the Bloom filter came from the sidecar file rather than a rebuild.
    bool bloom_loaded() const { return bloom_loaded_; }

    // False after opening a non-empty trie until its first write.
    bool counts_ready() const { std::shared_lock<std::shared_mutex> l(trie_latch_); return counts_ready_; }

    // insert returns the number of flips triggered. Returns 0 on duplicate key.
    size_t insert(const std::string& key, RecordPtr record);

    // Same, but the record is made once the key's place is known:
    // make_record(before, after) is called exactly once (never for a
    // duplicate), with the records of the entries just before and after the
    // key when it lands in a bucket (invalid otherwise), so the caller can
    // store the row near its key neighbours.
    using RecordFn = std::function<RecordPtr(RecordPtr before, RecordPtr after)>;
    size_t insert(const std::string& key, const RecordFn& make_record);

    // Point an existing key at a new record (its row moved). False if absent.
    bool set_record(const std::string& key, RecordPtr record);
    // set_record for many keys, e.g. the rows a heap page split moved (#68).
    // Keys sharing a bucket are updated with one descent and one write of that
    // bucket. Returns how many keys were found.
    size_t set_records(std::vector<std::pair<std::string, RecordPtr>> kvs);
    // remove returns true if the key existed and was deleted.
    bool   remove(const std::string& key);
    // lookup returns false if not found. ptr_out receives the record pointer if found.
    bool   lookup(const std::string& key,
                  RecordPtr* ptr_out    = nullptr,
                  size_t*    chains_out = nullptr) const;

    size_t total_flips() const { return total_flips_; }
    size_t bucket_bursts() const { return bucket_bursts_; }   // buckets rebuilt as chains
    size_t bucket_merges() const { return bucket_merges_; }   // chain subtrees collapsed into buckets
    size_t pruned()        const { return pruned_; }          // emptied light subtrees unlinked (#38)
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
    // Write every dirty cached chain to the file.
    void flush();

    // Compact: same as compact() but lays chains out in lexicographic key order.
    // Adjacent-in-key-order chains land in the same or adjacent packed blocks,
    // improving sequential range-scan I/O at the cost of point-lookup locality.
    void compact_lex();

    // Collect all (key, RecordPtr) pairs with lo <= key <= hi, in lex order.
    void range_scan(const std::string& lo, const std::string& hi,
                    std::vector<std::pair<std::string, RecordPtr>>& out) const;
    // The same keys in order, handed to fn (the key is valid during the call);
    // fn returns false to stop. Holds the trie's shared latch throughout.
    void range_visit(const std::string& lo, const std::string& hi,
                     const std::function<bool(const std::string& key, RecordPtr rec)>& fn) const;

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
    TrieOptions  opts_;
    std::unordered_map<uint64_t, ChainCounts> counts_;
    size_t total_flips_   = 0;
    size_t bucket_bursts_ = 0;
    size_t bucket_merges_ = 0;
    mutable std::shared_mutex trie_latch_; // shared=read/range, exclusive=write
    mutable std::atomic<uint64_t> cache_hits_{0};
    mutable std::atomic<uint64_t> cache_misses_{0};

    // ---- hot chain cache ----
    // Holds decoded ChainData for the most-accessed blocks.
    // Only writers change it, and they hold trie_latch_ exclusively; readers
    // (lookups, range scans) only read it under the shared latch and never add
    // to it (chain_read_shared), so it needs no lock of its own (#77).
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
    void flush_dirty   ();                  // write only the dirty entries (commit)

    bool                  durable_ = false;
    std::vector<uint64_t> freed_;            // slots freed since the last commit (durable_)
    void free_slot(uint64_t addr);           // free now, or at commit when durable_
    void release_freed();

    // Bloom filter for fast negative-lookup short-circuit (index/bloom.h).
public:
    const std::vector<BloomFilter::Stage>& bloom_stages() const { return bloom_.stages(); }
    bool bloom_may_contain_for_test(const std::string& key) const { return bloom_may_contain(key); }
private:
    BloomFilter          bloom_;   // empty until the first key
    std::string          bloom_path_;
    bool                 bloom_loaded_ = false;

    void   bloom_add (const std::string& key);
    void   rebuild_bloom();  // re-add every stored key
    bool   load_bloom();     // from bloom_path_; false if absent, invalid or stale
    void   save_bloom();     // to bloom_path_ (temp file + rename)
    bool   bloom_may_contain(const std::string& key) const; // false = definitely absent

    // Returns the current subtree key count for block_id, or 1 if unknown.
    // data (if given) supplies an estimate when counts_ isn't built yet.
    uint32_t hot_weight(uint64_t block_id, const ChainData* data = nullptr) const;

    ChainCounts&   get_counts(uint64_t block_id);
    void           init_counts(uint64_t block_id, uint32_t total, size_t num_nodes);
    static uint16_t floor_log2(uint32_t n);

    // Rebuild counts_ from disk after opening an existing trie file.
    void     rebuild_counts();
    // Per-chain counts only drive rebalancing, so an opened trie rebuilds them
    // on its first write instead of on open; reads never need them (hot_weight
    // treats unknown chains as weight 1). Call under the exclusive trie_latch_.
    void     ensure_counts();
    bool     counts_ready_ = true;
    uint32_t rebuild_chain_counts(uint64_t block_id);

    // Buckets edited in place in their mapped slot (#73). Each returns 0 when
    // addr is not a bucket or the edit needs the decoding path (the bucket
    // must move to a larger slot or burst); insert/erase/set return -1 for a
    // duplicate / missing key and 1 when done.
    bool is_cached(uint64_t addr) const;
    int  insert_in_bucket(uint64_t addr, const std::string& key, size_t key_pos,
                          const std::function<RecordPtr(RecordPtr, RecordPtr)>& take);
    int  erase_in_bucket(uint64_t addr, const std::string& key, size_t key_pos);
    int  set_in_bucket(uint64_t addr, const std::string& key, size_t key_pos, RecordPtr record);

    // set_record under an already held exclusive trie_latch_.
    bool set_record_locked(const std::string& key, RecordPtr record);

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


    // Swap the heavy continuation at nodes[ni] with its light child lights[li].
    // Returns false (no flip) if that child is a bucket: buckets are leaves.
    bool flip(uint64_t block_id, ChainData& chain, size_t ni, size_t li);

    // A node passed on the way to a key: light_digit < 0 = continued on the
    // heavy path, else went into the light child with that digit, whose
    // subtree starts at key nibble child_pos.
    // light_rank: the light child's index in the node's lights (-1 for the heavy side).
    struct Frame { uint64_t block_id; size_t ni; int light_digit; size_t child_pos = 0; int light_rank = -1; };

    // ---- the shared descent of insert, remove and set_record (#89) ----
    // Where a key stops in a decoded chain. scan_chain records a heavy frame
    // for every node the key passes on the chain's own path and, when the key
    // continues into a light child, that light's frame.
    struct ChainStop {
        enum class Kind {
            FOLLOW,     // continues into light child `child`, starting at key nibble `next_pos`
            ENDS,       // ends inside the path at `pos`; a node sits there iff has_node (index ni)
            TAIL,       // ends exactly where the path ends
            DIVERGES,   // differs from the path at `pos` (key digit `digit`); a node there
                        // (has_node, index ni) has no light child for that digit
            PAST_END,   // runs past the path; `digit` picks a child; an end node (has_node,
                        // index ni) exists but has no light child for it
        } kind;
        size_t   pos = 0, ni = 0, next_pos = 0;
        bool     has_node = false;
        uint8_t  digit = 0;
        uint64_t child = NULL_BLOCK;
    };
    ChainStop scan_chain(uint64_t block_id, const ChainData& chain, const std::string& key,
                         size_t key_pos, std::vector<Frame>& stack) const;

    // Walk from the root towards key: chains the key only passes are matched
    // in place, each bucket goes to in_bucket first (the operation's in-place
    // edit: 1 done, -1 stop, e.g. a duplicate or a missing key, 0 not handled
    // there), and other chains are decoded and scanned, following light
    // children, until the chain or bucket where the operation happens. Every
    // chain reached is appended to visited (bar a stopped in-place bucket).
    struct Descent {
        enum class End { IN_PLACE, BUCKET, CHAIN } end;
        int       in_place = 0;          // IN_PLACE: in_bucket's result
        uint64_t  block_id = NULL_BLOCK;
        size_t    key_pos  = 0;
        ChainData chain;                 // BUCKET, CHAIN: decoded
        ChainStop stop{};                // CHAIN
    };
    Descent descend(const std::string& key, std::vector<Frame>& stack, std::vector<uint64_t>& visited,
                    const std::function<int(uint64_t addr, size_t key_pos)>& in_bucket);

    // Match one chain or bucket without decoding it: in the hot cache, else on
    // the mapped bytes.
    MatchOutcome match_in_place(uint64_t addr, const std::string& key, size_t key_pos) const;
    bool pass_through(uint64_t& block_id, size_t& key_pos, const std::string& key,
                      std::vector<Frame>& stack, std::vector<uint64_t>& visited);


    // ---- buckets ----
    struct BuildItem { std::vector<uint8_t> nib; RecordPtr rec; };
    // Build the structure for sorted, unique items[lo,hi) sharing nibbles
    // [0, depth): a bucket if it fits, else a chain whose children are built
    // recursively (allocated). abs_start = absolute key nibble of depth.
    ChainData build_node(const std::vector<BuildItem>& items, size_t lo, size_t hi,
                         size_t depth, size_t abs_start, uint64_t hint, ChainCounts& counts);
    uint64_t  build_alloc(const std::vector<BuildItem>& items, size_t lo, size_t hi,
                          size_t depth, size_t abs_start, uint64_t hint);
    bool      fits_bucket(const std::vector<BuildItem>& items, size_t lo, size_t hi, size_t depth) const;
    // Every key under addr as (prefix + rest, record), plus every address in the subtree.
    void      collect_subtree(uint64_t addr, const std::vector<uint8_t>& prefix,
                              std::vector<BucketEntry>& out, std::vector<uint64_t>& addrs);
    // Replace the chain subtree at addr (starting at key nibble abs_start) with
    // one bucket in a slot of its own size class, if its keys fit, repointing
    // the parent (#17). Returns true if merged.
    // parent / ni / li locate the reference to addr: a light child of chain
    // parent, or the root when parent is NULL_BLOCK.
    bool      merge_to_bucket(uint64_t addr, size_t abs_start, uint64_t parent, size_t ni, int li);
    // Move a bucket that outgrew its slot to a new one, free the old slot and
    // repoint the parent, instead of leaving a forwarding stub (#17).
    uint64_t  move_bucket(uint64_t addr, const ChainData& bucket, uint64_t parent, size_t ni, int li);
    void      repoint_child(uint64_t parent, size_t ni, int li, uint64_t old_addr, uint64_t new_addr);
    void      prune_empty(std::vector<Frame>& stack);   // after a remove (#38)
    size_t    pruned_ = 0;
    size_t    insert_locked(const std::string& key, const RecordFn& make_record);

    // Walk stack from deepest to root, incrementing (insert) or decrementing
    // (remove) the relevant subtree count and rewriting weights on disk when
    // floor_log2 changes.  Flips when a light child outgrows the heavy side.
    void adjust_weights(const std::vector<Frame>& stack, bool increment);
};
