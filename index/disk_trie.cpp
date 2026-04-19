#include "index/disk_trie.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <shared_mutex>

// ---- bloom filter helpers ----
// Two independent 64-bit hashes (FNV-1a + Murmur-inspired mix) generate K bit
// positions via the double-hashing scheme: pos_i = (h1 + i*h2) % BLOOM_BITS.

static uint64_t bloom_h1(const std::string& key) {
    uint64_t h = 14695981039346656037ULL;
    for (unsigned char c : key) { h ^= c; h *= 1099511628211ULL; }
    return h;
}

static uint64_t bloom_h2(const std::string& key) {
    uint64_t h = 0;
    for (unsigned char c : key) { h = h * 31 + c; }
    h ^= h >> 33; h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33; h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h | 1; // must be odd so double-hashing covers all positions
}

void DiskTrie::bloom_add(const std::string& key) {
    if (bloom_.empty()) bloom_.assign(BLOOM_BYTES, 0);
    uint64_t h1 = bloom_h1(key), h2 = bloom_h2(key);
    for (int i = 0; i < BLOOM_K; i++) {
        size_t bit = (h1 + static_cast<uint64_t>(i) * h2) % BLOOM_BITS;
        bloom_[bit / 8] |= static_cast<uint8_t>(1u << (bit % 8));
    }
}

bool DiskTrie::bloom_may_contain(const std::string& key) const {
    if (bloom_.empty()) return true; // not yet initialised — assume present
    uint64_t h1 = bloom_h1(key), h2 = bloom_h2(key);
    for (int i = 0; i < BLOOM_K; i++) {
        size_t bit = (h1 + static_cast<uint64_t>(i) * h2) % BLOOM_BITS;
        if (!(bloom_[bit / 8] & static_cast<uint8_t>(1u << (bit % 8)))) return false;
    }
    return true;
}

static bool key_bit(const std::string& key, size_t abs) {
    return (static_cast<uint8_t>(key[abs / 8]) >> (7 - abs % 8)) & 1;
}

static bool path_bit(const ChainData& c, size_t i) {
    return (c.path_bits[i / 8] >> (7 - i % 8)) & 1;
}

static void set_path_bit(ChainData& c, size_t i, bool val) {
    uint8_t& byte = c.path_bits[i / 8];
    uint8_t  mask = 1u << (7 - i % 8);
    if (val) byte |= mask; else byte &= ~mask;
}

uint16_t DiskTrie::floor_log2(uint32_t n) {
    if (n <= 1) return 0;
    uint16_t k = 0;
    while ((1u << (k + 1)) <= n) ++k;
    return k;
}

ChainCounts& DiskTrie::get_counts(uint64_t block_id) {
    return counts_[block_id];
}

void DiskTrie::init_counts(uint64_t block_id, uint32_t total, size_t num_nodes) {
    auto& cc = counts_[block_id];
    cc.total = total;
    cc.nodes.assign(num_nodes, NodeCounts{});
}

static ChainData chain_from_key(const std::string& key, size_t start_bit,
                                 RecordPtr record) {
    ChainData c;
    size_t len      = key.size() * 8 - start_bit;
    c.path_bit_len  = static_cast<uint16_t>(len);
    c.bit_phase     = static_cast<uint8_t>(start_bit % 8);
    c.tail_record   = record;
    c.path_bits.resize((len + 7) / 8, 0);
    for (size_t i = 0; i < len; i++)
        set_path_bit(c, i, key_bit(key, start_bit + i));
    return c;
}

static void extend_chain(ChainData& c, const std::string& key,
                         size_t key_bit_start, RecordPtr record) {
    size_t extra = key.size() * 8 - key_bit_start;
    assert(extra > 0 && c.path_bit_len <= 255);
    ChainNode n{static_cast<uint8_t>(c.path_bit_len), 0, 0, NULL_BLOCK, c.tail_record};
    c.nodes.push_back(n);
    size_t new_len = c.path_bit_len + extra;
    c.path_bits.resize((new_len + 7) / 8, 0);
    for (size_t i = 0; i < extra; i++)
        set_path_bit(c, c.path_bit_len + i, key_bit(key, key_bit_start + i));
    c.path_bit_len = static_cast<uint16_t>(new_len);
    c.tail_record  = record;
}

void DiskTrie::flip(uint64_t block_id, ChainData& chain, size_t ni) {
    size_t    s       = chain.nodes[ni].split_bit;
    uint64_t  l_block = chain.nodes[ni].light_child_block;
    ChainData L       = chain_read(l_block);
    ChainCounts& cc   = get_counts(block_id);
    ChainCounts& lc   = get_counts(l_block);

    // 1. Build C_tail from chain's bits after s.
    ChainData c_tail;
    size_t tail_len      = chain.path_bit_len - s - 1;
    c_tail.path_bit_len  = static_cast<uint16_t>(tail_len);
    c_tail.bit_phase     = static_cast<uint8_t>((chain.bit_phase + s + 1) % 8);
    c_tail.tail_record   = chain.tail_record;
    c_tail.path_bits.resize((tail_len + 7) / 8, 0);
    for (size_t i = 0; i < tail_len; i++)
        set_path_bit(c_tail, i, path_bit(chain, s + 1 + i));
    for (size_t i = ni + 1; i < chain.nodes.size(); i++) {
        ChainNode n  = chain.nodes[i];
        n.split_bit -= static_cast<uint8_t>(s + 1);
        c_tail.nodes.push_back(n);
    }
    uint64_t c_tail_block = chain_alloc(c_tail, block_id);

    size_t tail_node_count = chain.nodes.size() - ni - 1;
    init_counts(c_tail_block, cc.nodes[ni].heavy, tail_node_count);
    for (size_t i = 0; i < tail_node_count; i++)
        get_counts(c_tail_block).nodes[i] = cc.nodes[ni + 1 + i];

    // 2. Rewrite chain's path from s onward with L's bits.
    size_t new_len = s + 1 + L.path_bit_len;
    chain.path_bits.resize((new_len + 7) / 8, 0);
    set_path_bit(chain, s, !path_bit(chain, s));
    for (size_t i = 0; i < L.path_bit_len; i++)
        set_path_bit(chain, s + 1 + i, path_bit(L, i));
    chain.path_bit_len = static_cast<uint16_t>(new_len);
    chain.tail_record  = L.tail_record;

    // 3. Replace nodes after ni with L's nodes (offsets shifted).
    chain.nodes.resize(ni + 1);
    for (ChainNode n : L.nodes) {
        n.split_bit += static_cast<uint8_t>(s + 1);
        chain.nodes.push_back(n);
    }

    // 4. Update branch node at ni.
    chain.nodes[ni].light_child_block  = c_tail_block;
    std::swap(cc.nodes[ni].light, cc.nodes[ni].heavy);
    chain.nodes[ni].light_child_weight = floor_log2(cc.nodes[ni].light);
    chain.nodes[ni].heavy_child_weight = floor_log2(cc.nodes[ni].heavy);

    // 5. Adopt L's node counts.
    cc.nodes.resize(ni + 1);
    for (const auto& nc : lc.nodes)
        cc.nodes.push_back(nc);

    chain_write(block_id, chain);
    total_flips_++;

    // L's block is now orphaned — discard from cache (no flush needed) and free.
    hot_discard(l_block);
    dm_.free_chain_slot(l_block);
}

ChainData DiskTrie::chain_read_shared(uint64_t block_id) const {
    auto it = hot_.find(block_id);
    if (it != hot_.end())
        return it->second.data;

    uint8_t slot = chain_addr_slot(block_id);
    if (slot == 0) {
        // Peek at the magic word to decide raw vs compressed.
        // Compressed chains (written by compaction) must go through
        // read_chain_at; raw chains can use the faster pinned-pointer path.
        const uint8_t* frame = dm_.pin_chain_shared(block_id);
        uint32_t magic;
        memcpy(&magic, frame, 4);
        dm_.unpin_chain_shared(block_id);

        if (magic == COMPRESS_MAGIC)
            return dm_.read_chain_at(block_id);

        frame = dm_.pin_chain_shared(block_id);
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
        std::lock_guard<std::mutex> lk(hot_mu_);
        auto it = hot_.find(block_id);
        if (it != hot_.end()) {
            cache_hits_.fetch_add(1, std::memory_order_relaxed);
            return it->second.data;
        }
    }
    cache_misses_.fetch_add(1, std::memory_order_relaxed);
    // Not cached: read from mmap (thread-safe, no lock held during I/O).
    ChainData data = dm_.read_chain(block_id);
    {
        std::lock_guard<std::mutex> lk(hot_mu_);
        if (!hot_.count(block_id)) { // another reader may have inserted it concurrently
            if (hot_.size() >= HOT_CAPACITY) hot_evict_one();
            hot_insert(block_id, HotEntry{data, 0, hot_weight(block_id)});
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
    uint32_t new_weight = hot_weight(block_id);
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
    if (hot_.size() >= HOT_CAPACITY)
        hot_evict_one();
    // Weight is unknown until init_counts is called by the caller; use 1 for now.
    hot_insert(id, HotEntry{chain, 0, 1});
    return id;
}

uint32_t DiskTrie::hot_weight(uint64_t block_id) const {
    auto it = counts_.find(block_id);
    return (it != counts_.end() && it->second.total > 0) ? it->second.total : 1;
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

uint32_t DiskTrie::rebuild_chain_counts(uint64_t block_id) {
    ChainData chain = dm_.read_chain(block_id);
    size_t num_nodes = chain.nodes.size();

    auto& cc = counts_[block_id];
    cc.nodes.assign(num_nodes, NodeCounts{});

    for (size_t ni = 0; ni < num_nodes; ni++) {
        uint64_t lb = chain.nodes[ni].light_child_block;
        if (lb != NULL_BLOCK)
            cc.nodes[ni].light = rebuild_chain_counts(lb);
    }

    uint32_t suffix = chain.tail_record.valid() ? 1 : 0;
    for (int ni = (int)num_nodes - 1; ni >= 0; ni--) {
        cc.nodes[ni].heavy = suffix;
        suffix += (chain.nodes[ni].record.valid() ? 1 : 0) + cc.nodes[ni].light;
    }
    cc.total = suffix;
    return suffix;
}

void DiskTrie::rebuild_counts() {
    rebuild_chain_counts(dm_.root_block());
}

static std::string bits_to_key(const std::vector<bool>& bits) {
    // At any record terminal, accumulated bits are always byte-aligned.
    std::string key(bits.size() / 8, '\0');
    for (size_t i = 0; i < bits.size(); i++)
        if (bits[i])
            key[i / 8] |= static_cast<char>(1u << (7 - i % 8));
    return key;
}

// ---- cursor-based in-order traversal ----
//
// advance_leftmost: pop the top frame from cursor.stack and find the
// leftmost (smallest) record in that subtree.  As we descend left,
// right-sibling subtrees are pushed onto cursor.stack so subsequent
// cursor_next() calls can visit them in order.
// Stack is LIFO: stack.back() = next subtree to visit.
// Ordering guarantee: push larger (1-branch) first, smaller (0-branch) last
// so that stack.back() is always the smallest remaining subtree.
static bool advance_leftmost(const DiskTrie& trie, TrieCursor& cursor,
                              std::string& key_out, RecordPtr& rec_out) {
    while (!cursor.stack.empty()) {
        auto frame = std::move(cursor.stack.back());
        cursor.stack.pop_back();

        ChainData chain = trie.chain_read(frame.block_id);

        // Find the first node whose split_bit is at or after our resume position.
        size_t ni = 0;
        while (ni < chain.nodes.size() &&
               chain.nodes[ni].split_bit < frame.cbit_start)
            ni++;

        std::vector<bool> prefix = std::move(frame.prefix);
        size_t bit_pos = frame.cbit_start;

        while (true) {
            const size_t next_split = (ni < chain.nodes.size())
                                      ? chain.nodes[ni].split_bit
                                      : chain.path_bit_len;

            // Accumulate path bits between current position and next split.
            for (size_t b = bit_pos; b < next_split; b++)
                prefix.push_back(path_bit(chain, b));
            bit_pos = next_split;

            if (ni == chain.nodes.size()) {
                // Tail of chain.
                if (chain.tail_record.valid()) {
                    key_out = bits_to_key(prefix);
                    rec_out = chain.tail_record;
                    return true;
                }
                break; // empty tail — continue with next stack frame
            }

            const ChainNode& node     = chain.nodes[ni];
            const bool       hbit     = path_bit(chain, node.split_bit); // heavy direction

            if (node.record.valid()) {
                // Record lives here (key ends exactly at this split bit).
                // Push both child subtrees so cursor_next() visits them in order.
                // Push the 1-branch (larger) first (deeper in stack), 0-branch last (top).
                auto p0 = prefix; p0.push_back(false);
                auto p1 = prefix; p1.push_back(true);

                if (!hbit) { // heavy=0, light=1
                    if (node.light_child_block != NULL_BLOCK)
                        cursor.stack.push_back({node.light_child_block, 0, std::move(p1)});
                    cursor.stack.push_back({frame.block_id, static_cast<size_t>(node.split_bit) + 1, std::move(p0)});
                } else {     // heavy=1, light=0
                    cursor.stack.push_back({frame.block_id, static_cast<size_t>(node.split_bit) + 1, std::move(p1)});
                    if (node.light_child_block != NULL_BLOCK)
                        cursor.stack.push_back({node.light_child_block, 0, std::move(p0)});
                }

                key_out = bits_to_key(prefix);
                rec_out = node.record;
                return true;
            }

            // No record here: descend into the 0-branch (smallest), push 1-branch for later.
            if (!hbit) { // heavy=0 = 0-branch; light=1 = 1-branch
                if (node.light_child_block != NULL_BLOCK) {
                    auto p1 = prefix; p1.push_back(true);
                    cursor.stack.push_back({node.light_child_block, 0, std::move(p1)});
                }
                prefix.push_back(false); // continue heavy (0-branch)
                bit_pos = static_cast<size_t>(node.split_bit) + 1;
                ni++;
            } else { // heavy=1 = 1-branch; light=0 = 0-branch
                {
                    auto p1 = prefix; p1.push_back(true);
                    cursor.stack.push_back({frame.block_id, static_cast<size_t>(node.split_bit) + 1, std::move(p1)});
                }
                if (node.light_child_block != NULL_BLOCK) {
                    prefix.push_back(false);
                    cursor.stack.push_back({node.light_child_block, 0, std::move(prefix)});
                }
                break; // outer loop will pop the light-child frame just pushed
            }
        }
    }
    return false;
}

// lower_bound_chain: walk chain at block_id matching lo bit-by-bit.
// Builds cursor.stack of right-sibling subtrees encountered along the path.
// On success, emits the first key >= lo through key_out/rec_out.
// prefix contains all key bits accumulated before entering this chain (cbit_start=0).
static bool lower_bound_chain(const DiskTrie& trie, uint64_t block_id,
                               std::vector<bool> prefix, const std::string& lo,
                               TrieCursor& cursor,
                               std::string& key_out, RecordPtr& rec_out) {
    const size_t lo_bits = lo.size() * 8;

    while (true) { // loop re-entered when we jump to a light child
        // If lo is already exhausted before entering this chain, the entire
        // subtree is >= lo — take leftmost.
        if (prefix.size() >= lo_bits) {
            cursor.stack.push_back({block_id, 0, std::move(prefix)});
            return advance_leftmost(trie, cursor, key_out, rec_out);
        }

        ChainData chain  = trie.chain_read(block_id);
        size_t    ni     = 0;
        bool      jumped = false;

        for (size_t i = 0; i < chain.path_bit_len; i++) {
            const bool has_node = (ni < chain.nodes.size() &&
                                   chain.nodes[ni].split_bit == i);
            // prefix.size() == total key bits accumulated == B + i
            // (B = bits before this chain; we push one bit per iteration on match paths)
            const size_t key_pos = prefix.size();

            // lo is exhausted here — everything from this position onward is >= lo.
            if (key_pos == lo_bits) {
                cursor.stack.push_back({block_id, i, std::move(prefix)});
                return advance_leftmost(trie, cursor, key_out, rec_out);
            }

            const bool lo_bit = key_bit(lo, key_pos);
            const bool cb     = path_bit(chain, i); // heavy direction at this bit

            if (has_node) {
                const ChainNode& node = chain.nodes[ni];
                // node.record: key ends at bit key_pos.  Since key_pos < lo_bits,
                // any such record's key is shorter than lo → record key < lo.  Skip.

                if (lo_bit == cb) {
                    // Follow heavy path (same direction as chain bit).
                    // If lo_bit==0 (cb==0): light(1-branch) > lo → push as right sibling.
                    // If lo_bit==1 (cb==1): light(0-branch) < lo → skip entirely.
                    if (!lo_bit && node.light_child_block != NULL_BLOCK) {
                        auto p1 = prefix; p1.push_back(true);
                        cursor.stack.push_back({node.light_child_block, 0, std::move(p1)});
                    }
                    prefix.push_back(cb);
                    ni++;
                } else if (lo_bit > cb) {
                    // lo_bit=1, cb=0: heavy(0) < lo at this bit — skip it.
                    // Jump to light child (bit=1 direction, same as lo_bit).
                    if (node.light_child_block == NULL_BLOCK)
                        return advance_leftmost(trie, cursor, key_out, rec_out);
                    prefix.push_back(true);
                    block_id = node.light_child_block;
                    jumped = true; break;
                } else {
                    // lo_bit=0, cb=1: heavy(1) > lo — push heavy as right sibling.
                    // Jump to light child (bit=0 direction, same as lo_bit).
                    {
                        auto p1 = prefix; p1.push_back(true);
                        cursor.stack.push_back({block_id, i + 1, std::move(p1)});
                    }
                    if (node.light_child_block == NULL_BLOCK)
                        return advance_leftmost(trie, cursor, key_out, rec_out);
                    prefix.push_back(false);
                    block_id = node.light_child_block;
                    jumped = true; break;
                }
            } else {
                // Non-split position: chain continues with exactly one bit value.
                if (lo_bit < cb) {
                    // Chain bit > lo bit → this and all deeper keys are > lo.
                    cursor.stack.push_back({block_id, i, std::move(prefix)});
                    return advance_leftmost(trie, cursor, key_out, rec_out);
                } else if (lo_bit > cb) {
                    // Chain bit < lo bit → no keys >= lo in this subtree.
                    return advance_leftmost(trie, cursor, key_out, rec_out);
                }
                prefix.push_back(cb); // matched
            }
        }

        if (jumped) continue; // restart with new block_id

        // Reached the tail of the chain.
        // prefix.size() == B + path_bit_len (all chain bits matched or walked).
        if (chain.tail_record.valid()) {
            const size_t tail_pos = prefix.size();
            if (tail_pos >= lo_bits) {
                // lo is a prefix of (or equal to) this key → key >= lo.
                key_out = bits_to_key(prefix);
                rec_out = chain.tail_record;
                return true;
            }
            // tail_pos < lo_bits: tail key is shorter than lo → tail key < lo.
        }
        // Nothing >= lo found along this path; try pending right siblings.
        return advance_leftmost(trie, cursor, key_out, rec_out);
    }
}

void DiskTrie::range_scan(const std::string& lo, const std::string& hi,
                          std::vector<std::pair<std::string, RecordPtr>>& out) const {
    std::shared_lock<std::shared_mutex> lock(trie_latch_);
    if (dm_.root_block() == NULL_BLOCK) return;

    TrieCursor  cursor;
    std::string key;
    RecordPtr   rec{NULL_BLOCK, 0};
    std::vector<bool> prefix;

    if (!lower_bound_chain(*this, dm_.root_block(), std::move(prefix), lo,
                           cursor, key, rec))
        return;
    if (key > hi) return;
    out.push_back({key, rec});

    while (advance_leftmost(*this, cursor, key, rec)) {
        if (key > hi) break;
        out.push_back({key, rec});
    }
}

bool DiskTrie::lower_bound(const std::string& lo, TrieCursor& cursor) const {
    std::shared_lock<std::shared_mutex> lock(trie_latch_);
    cursor = TrieCursor{};
    if (dm_.root_block() == NULL_BLOCK) return false;
    std::vector<bool> prefix;
    if (!lower_bound_chain(*this, dm_.root_block(), std::move(prefix), lo,
                           cursor, cursor.pending_key, cursor.pending_rec))
        return false;
    cursor.has_pending = true;
    return true;
}

bool DiskTrie::cursor_next(TrieCursor& cursor, std::string& key, RecordPtr& rec) const {
    if (cursor.has_pending) {
        key = std::move(cursor.pending_key);
        rec = cursor.pending_rec;
        cursor.has_pending = false;
        return true;
    }
    std::shared_lock<std::shared_mutex> lock(trie_latch_);
    return advance_leftmost(*this, cursor, key, rec);
}

size_t DiskTrie::active_chain_count() const {
    std::shared_lock<std::shared_mutex> lock(trie_latch_);
    if (dm_.root_block() == NULL_BLOCK) return 0;

    size_t count = 0;
    std::vector<uint64_t> stack = {dm_.root_block()};
    while (!stack.empty()) {
        uint64_t block_id = stack.back();
        stack.pop_back();
        count++;
        ChainData chain = chain_read_shared(block_id);
        for (const auto& node : chain.nodes)
            if (node.light_child_block != NULL_BLOCK)
                stack.push_back(node.light_child_block);
    }
    return count;
}

void DiskTrie::adjust_weights(const std::vector<Frame>& stack, bool increment) {
    for (int f = static_cast<int>(stack.size()) - 1; f >= 0; f--) {
        uint64_t bid   = stack[f].block_id;
        size_t   ni    = stack[f].ni;
        bool     light = stack[f].went_light;

        ChainData    chain = chain_read(bid);
        ChainCounts& cc    = get_counts(bid);

        uint32_t& side = light ? cc.nodes[ni].light : cc.nodes[ni].heavy;
        if (increment) side++;
        else if (side > 0) side--;

        uint16_t new_lw = floor_log2(cc.nodes[ni].light);
        uint16_t new_hw = floor_log2(cc.nodes[ni].heavy);
        if (new_lw != chain.nodes[ni].light_child_weight ||
            new_hw != chain.nodes[ni].heavy_child_weight) {
            chain.nodes[ni].light_child_weight = new_lw;
            chain.nodes[ni].heavy_child_weight = new_hw;
            chain_write(bid, chain);
        }

        // Flip when the side that just changed could cause imbalance:
        // insert went_light (light grew) or remove went_heavy (heavy shrank).
        bool check_flip = increment ? light : !light;
        if (check_flip && cc.nodes[ni].light > cc.nodes[ni].heavy)
            flip(bid, chain, ni);
    }
}

size_t DiskTrie::insert(const std::string& key, RecordPtr record) {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    bloom_add(key);
    size_t flips_before = total_flips_;

    if (dm_.root_block() == NULL_BLOCK) {
        ChainData c   = chain_from_key(key, 0, record);
        uint64_t root = chain_alloc(c);
        init_counts(root, 1, 0);
        dm_.set_root_block(root);
        return 0;
    }

    std::vector<Frame> stack;

    uint64_t block_id      = dm_.root_block();
    size_t   key_bit_start = 0;

    while (true) {
        ChainData chain = chain_read(block_id);
        get_counts(block_id).total++;
        size_t ni          = 0;
        bool   follow_light = false;
        bool   modified     = false;

        for (size_t i = 0; i < chain.path_bit_len; i++) {
            bool has_node = ni < chain.nodes.size() &&
                            chain.nodes[ni].split_bit == i;

            if (key_bit_start + i == key.size() * 8) {
                if (has_node && chain.nodes[ni].record.valid())
                    return total_flips_ - flips_before;
                ChainNode n{static_cast<uint8_t>(i), 0, 0, NULL_BLOCK, record};
                chain.nodes.insert(chain.nodes.begin() + ni, n);
                get_counts(block_id).nodes.insert(
                    get_counts(block_id).nodes.begin() + ni, NodeCounts{});
                chain_write(block_id, std::move(chain));
                modified = true;
                break;
            }

            bool kb = key_bit(key, key_bit_start + i);
            bool cb = path_bit(chain, i);

            if (has_node) {
                if (kb != cb) {
                    stack.push_back({block_id, ni, true});
                    block_id      = chain.nodes[ni].light_child_block;
                    key_bit_start = key_bit_start + i + 1;
                    follow_light  = true;
                    break;
                }
                stack.push_back({block_id, ni, false});
                ni++;
            } else {
                if (kb != cb) {
                    uint32_t count_at_i = get_counts(block_id).total - 1;
                    for (size_t j = 0; j < ni; j++)
                        count_at_i -= get_counts(block_id).nodes[j].light;

                    uint64_t new_block = chain_alloc(
                        chain_from_key(key, key_bit_start + i + 1, record), block_id);
                    init_counts(new_block, 1, 0);

                    ChainNode n{static_cast<uint8_t>(i), 0,
                                floor_log2(count_at_i), new_block, RecordPtr{}};
                    chain.nodes.insert(chain.nodes.begin() + ni, n);
                    NodeCounts nc{1, count_at_i};
                    get_counts(block_id).nodes.insert(
                        get_counts(block_id).nodes.begin() + ni, nc);
                    chain_write(block_id, std::move(chain));
                    modified = true;
                    break;
                }
            }
        }

        if (follow_light) continue;
        if (modified)     break;

        if (key_bit_start + chain.path_bit_len == key.size() * 8) {
            if (chain.tail_record.valid())
                return total_flips_ - flips_before;
            chain.tail_record = record;
            chain_write(block_id, std::move(chain));
            break;
        }

        {
            size_t old_node_count = chain.nodes.size();
            extend_chain(chain, key, key_bit_start + chain.path_bit_len, record);
            get_counts(block_id).nodes.push_back(NodeCounts{});
            assert(chain.nodes.size() == old_node_count + 1);
        }
        chain_write(block_id, std::move(chain));
        break;
    }

    adjust_weights(stack, true);
    return total_flips_ - flips_before;
}

bool DiskTrie::insert_one_no_rebalance(DiskTrie& trie,
                                     DiskManager& dm,
                                     const std::string& key,
                                     RecordPtr record) {
    if (dm.root_block() == NULL_BLOCK) {
        ChainData c   = chain_from_key(key, 0, record);
        uint64_t root = trie.chain_alloc(c);
        dm.set_root_block(root);
        return true;
    }

    uint64_t block_id      = dm.root_block();
    size_t   key_bit_start = 0;

    while (true) {
        ChainData chain     = trie.chain_read(block_id);
        size_t    ni        = 0;
        bool      follow_light = false;

        for (size_t i = 0; i < chain.path_bit_len; i++) {
            bool has_node = ni < chain.nodes.size() &&
                            chain.nodes[ni].split_bit == i;

            if (key_bit_start + i == key.size() * 8) {
                if (has_node && chain.nodes[ni].record.valid()) return false;
                ChainNode n{static_cast<uint8_t>(i), 0, 0, NULL_BLOCK, record};
                chain.nodes.insert(chain.nodes.begin() + ni, n);
                trie.chain_write(block_id, std::move(chain));
                return true;
            }

            bool kb = key_bit(key, key_bit_start + i);
            bool cb = path_bit(chain, i);

            if (has_node) {
                if (kb != cb) {
                    block_id      = chain.nodes[ni].light_child_block;
                    key_bit_start = key_bit_start + i + 1;
                    follow_light  = true;
                    break;
                }
                ni++;
            } else {
                if (kb != cb) {
                    uint64_t new_block = trie.chain_alloc(
                        chain_from_key(key, key_bit_start + i + 1, record), block_id);
                    ChainNode n{static_cast<uint8_t>(i), 0, 0, new_block, RecordPtr{}};
                    chain.nodes.insert(chain.nodes.begin() + ni, n);
                    trie.chain_write(block_id, std::move(chain));
                    return true;
                }
            }
        }

        if (follow_light) continue;

        if (key_bit_start + chain.path_bit_len == key.size() * 8) {
            if (chain.tail_record.valid()) return false;
            chain.tail_record = record;
            trie.chain_write(block_id, std::move(chain));
            return true;
        }

        {
            size_t old_node_count = chain.nodes.size();
            extend_chain(chain, key, key_bit_start + chain.path_bit_len, record);
            assert(chain.nodes.size() == old_node_count + 1);
        }
        trie.chain_write(block_id, std::move(chain));
        return true;
    }
}

void DiskTrie::bulk_insert(std::vector<std::pair<std::string, RecordPtr>> kvs) {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    counts_.clear();
    hot_.clear();
    hot_clean_.clear();
    hot_dirty_.clear();
    hot_wpos_.clear();

    for (auto& [key, ptr] : kvs) {
        bloom_add(key);
        insert_one_no_rebalance(*this, dm_, key, ptr);
    }

    hot_flush_all();

    // Rebuild counts_ from the new trie structure in one DFS pass.
    if (dm_.root_block() != NULL_BLOCK)
        rebuild_chain_counts(dm_.root_block());

    // Now fix up the on-disk weights to match the rebuilt counts.
    // Walk every chain and rewrite light_child_weight / heavy_child_weight.
    std::vector<uint64_t> stack = {dm_.root_block()};
    while (!stack.empty()) {
        uint64_t bid = stack.back(); stack.pop_back();
        ChainData chain = chain_read(bid);
        ChainCounts& cc = counts_[bid];
        bool changed = false;
        for (size_t ni = 0; ni < chain.nodes.size(); ni++) {
            uint16_t lw = floor_log2(cc.nodes[ni].light);
            uint16_t hw = floor_log2(cc.nodes[ni].heavy);
            if (chain.nodes[ni].light_child_weight != lw ||
                chain.nodes[ni].heavy_child_weight != hw) {
                chain.nodes[ni].light_child_weight = lw;
                chain.nodes[ni].heavy_child_weight = hw;
                changed = true;
            }
            if (chain.nodes[ni].light_child_block != NULL_BLOCK)
                stack.push_back(chain.nodes[ni].light_child_block);
        }
        if (changed) chain_write(bid, std::move(chain));
    }
    hot_flush_all();
}

void DiskTrie::compact_assign(uint64_t old_addr, uint64_t parent_new_phys,
                               std::unordered_map<uint64_t, uint64_t>& remap) {
    if (old_addr == NULL_BLOCK) return;

    ChainData chain   = dm_.read_chain_at(old_addr);
    uint64_t new_addr = dm_.alloc_chain_slot(chain, parent_new_phys);
    remap[old_addr]   = new_addr;

    uint64_t my_phys = chain_addr_phys(new_addr);
    for (auto& node : chain.nodes)
        compact_assign(node.light_child_block, my_phys, remap);
}

void DiskTrie::compact_apply(uint64_t old_root,
                              std::unordered_map<uint64_t, uint64_t>& remap) {
    // Pass 2: rewrite each new slot with updated light_child_block pointers,
    // compressing the chain data (delta + zstd) as we go.
    for (auto& [old_addr, new_addr] : remap) {
        ChainData chain = dm_.read_chain_at(old_addr);
        for (auto& node : chain.nodes)
            if (node.light_child_block != NULL_BLOCK)
                node.light_child_block = remap.at(node.light_child_block);
        dm_.update_chain_at_compressed(new_addr, chain);
    }

    // Pass 3: re-key hot cache + counts_ from old→new addresses, fix up
    // light_child_block pointers inside cached ChainData, then free old slots.
    {
        std::unordered_map<uint64_t, HotEntry>  new_hot;
        std::multimap<uint32_t, uint64_t>        new_clean, new_dirty;
        std::unordered_map<uint64_t, std::multimap<uint32_t,uint64_t>::iterator> new_wpos;

        for (auto& [old_addr, new_addr] : remap) {
            auto it = hot_.find(old_addr);
            if (it != hot_.end()) {
                HotEntry& e = it->second;
                for (auto& node : e.data.nodes) {
                    if (node.light_child_block != NULL_BLOCK) {
                        auto r = remap.find(node.light_child_block);
                        if (r != remap.end())
                            node.light_child_block = r->second;
                    }
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
}

void DiskTrie::compact() {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
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

    // Allocate light subtrees that are lex-smaller than this chain's heavy path.
    // That happens when the heavy bit at the split is 1 (light takes 0 = smaller).
    for (auto& node : chain.nodes)
        if (path_bit(chain, node.split_bit) && node.light_child_block != NULL_BLOCK)
            compact_assign_lex(node.light_child_block, last_phys, remap);

    // Allocate this chain adjacent to its lex-predecessor.
    uint64_t new_addr = dm_.alloc_chain_slot(chain, last_phys);
    remap[old_addr]   = new_addr;
    last_phys         = chain_addr_phys(new_addr);

    // Allocate light subtrees that are lex-larger than this chain's heavy path.
    // That happens when the heavy bit at the split is 0 (light takes 1 = larger).
    for (auto& node : chain.nodes)
        if (!path_bit(chain, node.split_bit) && node.light_child_block != NULL_BLOCK)
            compact_assign_lex(node.light_child_block, last_phys, remap);
}

void DiskTrie::compact_lex() {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
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

bool DiskTrie::remove(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    if (dm_.root_block() == NULL_BLOCK) return false;

    std::vector<Frame> stack;

    uint64_t block_id      = dm_.root_block();
    size_t   key_bit_start = 0;

    while (true) {
        ChainData chain     = chain_read(block_id);
        size_t    ni        = 0;
        bool      follow_light = false;
        bool      modified     = false;

        for (size_t i = 0; i < chain.path_bit_len; i++) {
            bool has_node = ni < chain.nodes.size() &&
                            chain.nodes[ni].split_bit == i;

            if (key_bit_start + i == key.size() * 8) {
                if (!has_node || !chain.nodes[ni].record.valid()) return false;
                chain.nodes[ni].record = RecordPtr{};
                chain_write(block_id, std::move(chain));
                modified = true;
                break;
            }

            bool kb = key_bit(key, key_bit_start + i);
            bool cb = path_bit(chain, i);

            if (has_node) {
                if (kb != cb) {
                    stack.push_back({block_id, ni, true});
                    block_id      = chain.nodes[ni].light_child_block;
                    key_bit_start = key_bit_start + i + 1;
                    follow_light  = true;
                    break;
                }
                stack.push_back({block_id, ni, false});
                ni++;
            } else {
                if (kb != cb) return false;
            }
        }

        if (follow_light) continue;
        if (modified)     break;

        if (key_bit_start + chain.path_bit_len == key.size() * 8) {
            if (!chain.tail_record.valid()) return false;
            chain.tail_record = RecordPtr{};
            chain_write(block_id, std::move(chain));
            break;
        }

        return false;
    }

    adjust_weights(stack, false);
    return true;
}

bool DiskTrie::lookup(const std::string& key, RecordPtr* ptr_out,
                      size_t* chains_out) const {
    if (!bloom_may_contain(key)) return false;
    std::shared_lock<std::shared_mutex> lock(trie_latch_);
    if (dm_.root_block() == NULL_BLOCK) return false;

    size_t   chains  = 1;
    uint64_t cur     = dm_.root_block();
    size_t   key_pos = 0;

    while (cur != NULL_BLOCK) {
        ChainData chain  = chain_read_shared(cur);
        MatchOutcome r   = chain_match(chain, key, key_pos);

        switch (r.kind) {
            case MatchOutcome::Kind::FOUND:
                if (ptr_out)    *ptr_out    = r.record;
                if (chains_out) *chains_out = chains;
                return true;
            case MatchOutcome::Kind::MISS:
                return false;
            case MatchOutcome::Kind::FOLLOW_LIGHT:
                cur     = chain.nodes[r.node_index].light_child_block;
                key_pos = r.next_key_bit;
                chains++;
                break;
        }
    }
    return false;
}
