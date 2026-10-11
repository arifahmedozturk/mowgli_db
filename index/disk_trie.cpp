#include "index/disk_trie.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <shared_mutex>
#include <stdexcept>

// The trie is base 16: keys are matched one nibble at a time (see chain.h).
// A chain is a heavy path; each node on it branches into up to 15 light
// children, and a lookup crosses O(log n) chains because a light child holds
// at most half of its parent's keys.

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

void DiskTrie::rebuild_bloom() {
    TrieCursor  cursor;
    std::string key;
    RecordPtr   rec;
    if (!lower_bound("", cursor)) return;
    while (cursor_next(cursor, key, rec)) bloom_add(key);
}

// Sidecar layout: [magic u32][bytes u32][key_count u64][filter bytes].
// key_count must match the trie header, which rejects a filter saved before
// later writes (e.g. a sidecar copied back or left by an older close).
static constexpr uint32_t BLOOM_FILE_MAGIC = 0x424C4D31; // "BLM1"

bool DiskTrie::load_bloom() {
    if (bloom_path_.empty()) return false;
    FILE* f = std::fopen(bloom_path_.c_str(), "rb");
    if (!f) return false;
    uint32_t magic = 0, bytes = 0;
    uint64_t keys  = 0;
    std::vector<uint8_t> data(BLOOM_BYTES);
    bool ok = std::fread(&magic, 4, 1, f) == 1 && magic == BLOOM_FILE_MAGIC
           && std::fread(&bytes, 4, 1, f) == 1 && bytes == BLOOM_BYTES
           && std::fread(&keys,  8, 1, f) == 1 && keys == dm_.key_count()
           && std::fread(data.data(), 1, BLOOM_BYTES, f) == BLOOM_BYTES;
    std::fclose(f);
    if (ok) bloom_ = std::move(data);
    return ok;
}

void DiskTrie::save_bloom() {
    if (bloom_path_.empty() || bloom_.empty()) return;
    std::string tmp = bloom_path_ + ".tmp";
    FILE* f = std::fopen(tmp.c_str(), "wb");
    if (!f) return;  // best effort: the next open rebuilds
    uint32_t magic = BLOOM_FILE_MAGIC, bytes = BLOOM_BYTES;
    uint64_t keys  = dm_.key_count();
    bool ok = std::fwrite(&magic, 4, 1, f) == 1 && std::fwrite(&bytes, 4, 1, f) == 1
           && std::fwrite(&keys, 8, 1, f) == 1
           && std::fwrite(bloom_.data(), 1, bloom_.size(), f) == bloom_.size();
    ok = std::fclose(f) == 0 && ok;
    if (ok) std::rename(tmp.c_str(), bloom_path_.c_str());
    else    std::remove(tmp.c_str());
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

// ---- chain construction helpers ----

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

static size_t key_nibbles(const std::string& key) { return key.size() * 2; }

// A chain holding the rest of key from nibble start, ending in record.
static ChainData chain_from_key(const std::string& key, size_t start, RecordPtr record) {
    ChainData c;
    size_t len    = key_nibbles(key) - start;
    if (len > UINT16_MAX) throw std::runtime_error("key too long for a chain");
    c.path_len    = static_cast<uint16_t>(len);
    c.phase       = static_cast<uint8_t>(start % 2);
    c.tail_record = record;
    c.path.resize(len);
    for (size_t i = 0; i < len; i++) c.path[i] = key_nibble(key, start + i);
    return c;
}

// Extend a chain whose path is a strict prefix of key (from nibble key_pos at
// the chain's end): the old tail key, if any, becomes a record on a new node.
// Returns true if a node was added.
static bool extend_chain(ChainData& c, const std::string& key, size_t key_pos, RecordPtr record) {
    size_t extra = key_nibbles(key) - key_pos;
    assert(extra > 0);
    if (c.path_len + extra > UINT16_MAX) throw std::runtime_error("key too long for a chain");
    bool node = c.tail_record.valid();
    if (node) {
        ChainNode n;
        n.split  = c.path_len;
        n.record = c.tail_record;
        c.nodes.push_back(n);
    }
    for (size_t i = 0; i < extra; i++) c.path.push_back(key_nibble(key, key_pos + i));
    c.path_len    = static_cast<uint16_t>(c.path.size());
    c.tail_record = record;
    return node;
}

// Insert a light child keeping lights sorted by digit; returns its index.
static size_t insert_light(ChainNode& node, LightChild l) {
    auto it = std::lower_bound(node.lights.begin(), node.lights.end(), l.digit,
                               [](const LightChild& a, uint8_t d) { return a.digit < d; });
    size_t at = static_cast<size_t>(it - node.lights.begin());
    node.lights.insert(it, l);
    return at;
}

void DiskTrie::flip(uint64_t block_id, ChainData& chain, size_t ni, size_t li) {
    ChainNode&   node     = chain.nodes[ni];
    size_t       s        = node.split;
    uint8_t      h        = chain.path[s];
    LightChild   light    = node.lights[li];
    ChainData    L        = chain_read(light.block);
    ChainCounts& cc       = get_counts(block_id);
    std::vector<NodeCounts> l_nodes = get_counts(light.block).nodes;
    uint32_t old_heavy = cc.nodes[ni].heavy;
    uint32_t old_light = cc.nodes[ni].lights[li];

    // 1. The chain's continuation after s becomes a new chain, C_tail.
    ChainData c_tail;
    c_tail.path.assign(chain.path.begin() + static_cast<ptrdiff_t>(s) + 1, chain.path.end());
    c_tail.path_len    = static_cast<uint16_t>(c_tail.path.size());
    c_tail.phase       = static_cast<uint8_t>((chain.phase + s + 1) % 2);
    c_tail.tail_record = chain.tail_record;
    for (size_t i = ni + 1; i < chain.nodes.size(); i++) {
        ChainNode n = chain.nodes[i];
        n.split = static_cast<uint16_t>(n.split - (s + 1));
        c_tail.nodes.push_back(std::move(n));
    }
    uint64_t c_tail_block = chain_alloc(c_tail, block_id);
    size_t tail_node_count = chain.nodes.size() - ni - 1;
    init_counts(c_tail_block, old_heavy, tail_node_count);
    for (size_t i = 0; i < tail_node_count; i++)
        get_counts(c_tail_block).nodes[i] = cc.nodes[ni + 1 + i];

    // 2. The chain now continues into L: digit at s, then L's path and nodes.
    chain.path.resize(s + 1);
    chain.path[s] = light.digit;
    chain.path.insert(chain.path.end(), L.path.begin(), L.path.end());
    chain.path_len    = static_cast<uint16_t>(chain.path.size());
    chain.tail_record = L.tail_record;
    chain.nodes.resize(ni + 1);
    for (ChainNode n : L.nodes) {
        n.split = static_cast<uint16_t>(n.split + s + 1);
        chain.nodes.push_back(std::move(n));
    }

    // 3. At node ni the old heavy side is now a light child (digit h).
    ChainNode& nd = chain.nodes[ni];
    nd.lights.erase(nd.lights.begin() + static_cast<ptrdiff_t>(li));
    size_t new_li = insert_light(nd, {h, floor_log2(old_heavy), c_tail_block});
    nd.heavy_weight = floor_log2(old_light);

    NodeCounts& nc = cc.nodes[ni];
    nc.lights.erase(nc.lights.begin() + static_cast<ptrdiff_t>(li));
    nc.lights.insert(nc.lights.begin() + static_cast<ptrdiff_t>(new_li), old_heavy);
    nc.heavy = old_light;
    cc.nodes.resize(ni + 1);
    cc.nodes.insert(cc.nodes.end(), l_nodes.begin(), l_nodes.end());

    chain_write(block_id, chain);
    total_flips_++;

    // L's slot is now orphaned — discard from cache (no flush needed) and free.
    hot_discard(light.block);
    counts_.erase(light.block);
    dm_.free_chain_slot(light.block);
}

// ---- hot chain cache ----

ChainData DiskTrie::chain_read_shared(uint64_t block_id) const {
    {
        // Range-scan cursors insert/evict hot_ under a shared trie_latch_, so reads need hot_mu_ too.
        std::lock_guard<std::mutex> lk(hot_mu_);
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
    if (hot_.size() >= HOT_CAPACITY)
        hot_evict_one();
    // Weight is unknown until init_counts is called by the caller; use 1 for now.
    hot_insert(id, HotEntry{chain, 0, 1});
    return id;
}

uint32_t DiskTrie::hot_weight(uint64_t block_id, const ChainData* data) const {
    auto it = counts_.find(block_id);
    if (it != counts_.end() && it->second.total > 0) return it->second.total;
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
    size_t num_nodes = chain.nodes.size();

    std::vector<NodeCounts> nodes(num_nodes);
    for (size_t ni = 0; ni < num_nodes; ni++)
        for (const auto& l : chain.nodes[ni].lights)
            nodes[ni].lights.push_back(rebuild_chain_counts(l.block));

    uint32_t suffix = chain.tail_record.valid() ? 1 : 0;
    for (int ni = (int)num_nodes - 1; ni >= 0; ni--) {
        nodes[ni].heavy = suffix;
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
    std::lock_guard<std::mutex> lk(hot_mu_);
    hot_clean_.clear();
    hot_dirty_.clear();
    hot_wpos_.clear();
    for (auto& [id, entry] : hot_) {
        entry.weight  = hot_weight(id, &entry.data);
        auto& wmap    = entry.dirty > 0 ? hot_dirty_ : hot_clean_;
        hot_wpos_[id] = wmap.insert({entry.weight, id});
    }
}

// ---- cursor-based in-order traversal ----

// At any record, the accumulated nibbles are byte-aligned (an even count).
static std::string nibbles_to_key(const std::vector<uint8_t>& nibbles) {
    std::string key(nibbles.size() / 2, '\0');
    for (size_t i = 0; i + 1 < nibbles.size(); i += 2)
        key[i / 2] = static_cast<char>(nibbles[i] << 4 | nibbles[i + 1]);
    return key;
}

// The subtrees below a chain node, in key order: its light children plus the
// chain's own continuation (the heavy digit).
struct ChildRef { uint8_t digit; bool heavy; uint64_t block; };

static std::vector<ChildRef> node_children(const ChainData& chain, const ChainNode& node) {
    std::vector<ChildRef> kids;
    kids.reserve(node.lights.size() + 1);
    uint8_t h = chain.path[node.split];
    bool placed = false;
    for (const auto& l : node.lights) {
        if (!placed && h < l.digit) { kids.push_back({h, true, NULL_BLOCK}); placed = true; }
        kids.push_back({l.digit, false, l.block});
    }
    if (!placed) kids.push_back({h, true, NULL_BLOCK});
    return kids;
}

// Push the subtree for kid onto the cursor stack.
static void push_child(TrieCursor& cursor, uint64_t chain_block, const ChainNode& node,
                       const ChildRef& kid, std::vector<uint8_t> prefix) {
    prefix.push_back(kid.digit);
    if (kid.heavy) cursor.stack.push_back({chain_block, static_cast<size_t>(node.split) + 1, std::move(prefix)});
    else           cursor.stack.push_back({kid.block, 0, std::move(prefix)});
}

// advance_leftmost: pop the top frame from cursor.stack and find the leftmost
// (smallest) record in that subtree. Larger sibling subtrees met on the way
// down are pushed so later cursor_next() calls visit them in order.
// Stack is LIFO: stack.back() is always the smallest remaining subtree.
static bool advance_leftmost(const DiskTrie& trie, TrieCursor& cursor,
                              std::string& key_out, RecordPtr& rec_out) {
    while (!cursor.stack.empty()) {
        auto frame = std::move(cursor.stack.back());
        cursor.stack.pop_back();

        ChainData chain = trie.chain_read(frame.block_id);

        // First node at or after our resume position.
        size_t ni = 0;
        while (ni < chain.nodes.size() && chain.nodes[ni].split < frame.cpos_start) ni++;

        std::vector<uint8_t> prefix = std::move(frame.prefix);
        size_t pos = frame.cpos_start;

        while (true) {
            const size_t next_split = (ni < chain.nodes.size()) ? chain.nodes[ni].split
                                                                : chain.path_len;
            prefix.insert(prefix.end(), chain.path.begin() + static_cast<ptrdiff_t>(pos),
                          chain.path.begin() + static_cast<ptrdiff_t>(next_split));
            pos = next_split;

            if (ni == chain.nodes.size()) {
                if (chain.tail_record.valid()) {
                    key_out = nibbles_to_key(prefix);
                    rec_out = chain.tail_record;
                    return true;
                }
                break; // empty tail — continue with next stack frame
            }

            const ChainNode& node = chain.nodes[ni];
            std::vector<ChildRef> kids = node_children(chain, node);

            if (node.record.valid()) {
                // The key ending here is smaller than every child subtree.
                for (size_t k = kids.size(); k-- > 0;)
                    push_child(cursor, frame.block_id, node, kids[k], prefix);
                key_out = nibbles_to_key(prefix);
                rec_out = node.record;
                return true;
            }

            // Visit the smallest child next; push the rest for later.
            for (size_t k = kids.size(); k-- > 1;)
                push_child(cursor, frame.block_id, node, kids[k], prefix);
            if (kids[0].heavy) {
                prefix.push_back(kids[0].digit);
                pos = static_cast<size_t>(node.split) + 1;
                ni++;
            } else {
                push_child(cursor, frame.block_id, node, kids[0], std::move(prefix));
                break; // outer loop pops the light frame just pushed
            }
        }
    }
    return false;
}

// lower_bound_chain: walk chains matching lo nibble by nibble, pushing every
// subtree greater than lo met on the way; then emit the first key >= lo.
static bool lower_bound_chain(const DiskTrie& trie, uint64_t block_id,
                               std::vector<uint8_t> prefix, const std::string& lo,
                               TrieCursor& cursor,
                               std::string& key_out, RecordPtr& rec_out) {
    const size_t lo_n = key_nibbles(lo);

    while (true) { // re-entered when we jump to a light child
        // lo already exhausted: the whole subtree is >= lo.
        if (prefix.size() >= lo_n) {
            cursor.stack.push_back({block_id, 0, std::move(prefix)});
            return advance_leftmost(trie, cursor, key_out, rec_out);
        }

        ChainData chain  = trie.chain_read(block_id);
        size_t    ni     = 0;
        bool      jumped = false;

        for (size_t i = 0; i < chain.path_len; i++) {
            const bool   has_node = ni < chain.nodes.size() && chain.nodes[ni].split == i;
            const size_t key_pos  = prefix.size();

            // lo is exhausted here — everything from this position on is >= lo.
            if (key_pos == lo_n) {
                cursor.stack.push_back({block_id, i, std::move(prefix)});
                return advance_leftmost(trie, cursor, key_out, rec_out);
            }

            const uint8_t lo_d = key_nibble(lo, key_pos);
            const uint8_t cd   = chain.path[i];

            if (has_node) {
                // A record here is a key shorter than lo, so smaller: skip it.
                const ChainNode& node = chain.nodes[ni];
                std::vector<ChildRef> kids = node_children(chain, node);
                for (size_t k = kids.size(); k-- > 0;)
                    if (kids[k].digit > lo_d) push_child(cursor, block_id, node, kids[k], prefix);

                if (lo_d == cd) { prefix.push_back(cd); ni++; continue; }
                int li = node.light_index(lo_d);
                if (li < 0) return advance_leftmost(trie, cursor, key_out, rec_out);
                prefix.push_back(lo_d);
                block_id = node.lights[li].block;
                jumped = true;
                break;
            }
            if (lo_d < cd) {
                // The chain's digit is above lo's: this and all deeper keys are > lo.
                cursor.stack.push_back({block_id, i, std::move(prefix)});
                return advance_leftmost(trie, cursor, key_out, rec_out);
            }
            if (lo_d > cd) return advance_leftmost(trie, cursor, key_out, rec_out);
            prefix.push_back(cd);
        }

        if (jumped) continue;

        // Reached the chain's tail with every nibble matched.
        if (chain.tail_record.valid() && prefix.size() >= lo_n) {
            key_out = nibbles_to_key(prefix);
            rec_out = chain.tail_record;
            return true;
        }
        // The tail key (if any) is shorter than lo; try pending larger siblings.
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

    if (!lower_bound_chain(*this, dm_.root_block(), {}, lo, cursor, key, rec))
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
    if (!lower_bound_chain(*this, dm_.root_block(), {}, lo,
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
            for (const auto& l : node.lights) stack.push_back(l.block);
    }
    return count;
}

// ---- insert / remove ----

void DiskTrie::adjust_weights(const std::vector<Frame>& stack, bool increment) {
    for (int f = static_cast<int>(stack.size()) - 1; f >= 0; f--) {
        uint64_t bid = stack[f].block_id;
        size_t   ni  = stack[f].ni;

        ChainData    chain = chain_read(bid);
        ChainCounts& cc    = get_counts(bid);
        ChainNode&   node  = chain.nodes[ni];
        NodeCounts&  nc    = cc.nodes[ni];

        int li = stack[f].light_digit < 0 ? -1 : node.light_index(static_cast<uint8_t>(stack[f].light_digit));
        if (stack[f].light_digit >= 0 && li < 0) continue;  // child gone (cannot happen)
        uint32_t& side = li < 0 ? nc.heavy : nc.lights[li];
        if (increment) side++;
        else if (side > 0) side--;

        uint16_t new_w   = floor_log2(side);
        uint16_t& stored = li < 0 ? node.heavy_weight : node.lights[li].weight;
        if (stored != new_w) {
            stored = new_w;
            chain_write(bid, chain);
        }

        // Flip when a light child now holds more keys than the heavy side:
        // after an insert into that light child, or a remove from the heavy side.
        if (increment && li >= 0) {
            if (nc.lights[li] > nc.heavy) flip(bid, chain, ni, static_cast<size_t>(li));
        } else if (!increment && li < 0) {
            size_t best = 0;
            for (size_t k = 1; k < nc.lights.size(); k++)
                if (nc.lights[k] > nc.lights[best]) best = k;
            if (!nc.lights.empty() && nc.lights[best] > nc.heavy) flip(bid, chain, ni, best);
        }
    }
}

size_t DiskTrie::insert(const std::string& key, RecordPtr record) {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    ensure_counts();
    size_t flips_before = total_flips_;
    const size_t kn = key_nibbles(key);

    if (dm_.root_block() == NULL_BLOCK) {
        bloom_add(key);
        ChainData c   = chain_from_key(key, 0, record);
        uint64_t root = chain_alloc(c);
        init_counts(root, 1, 0);
        dm_.set_root_block(root);
        return 0;
    }

    std::vector<Frame>    stack;
    std::vector<uint64_t> visited;   // chains whose totals grow if the insert succeeds
    uint64_t block_id = dm_.root_block();
    size_t   key_pos  = 0;

    while (true) {
        ChainData chain = chain_read(block_id);
        visited.push_back(block_id);
        size_t ni   = 0;
        bool   next = false, done = false;

        for (size_t i = 0; i < chain.path_len && !next && !done; i++) {
            bool has_node = ni < chain.nodes.size() && chain.nodes[ni].split == i;

            if (key_pos + i == kn) {
                // The key ends inside this chain, at position i.
                if (has_node) {
                    if (chain.nodes[ni].record.valid()) return 0;  // duplicate
                    chain.nodes[ni].record = record;
                } else {
                    // Keys continuing past i on the chain = those past the previous node.
                    uint32_t passing = ni > 0 ? get_counts(block_id).nodes[ni - 1].heavy
                                              : get_counts(block_id).total;
                    ChainNode n;
                    n.split        = static_cast<uint16_t>(i);
                    n.heavy_weight = floor_log2(passing);
                    n.record       = record;
                    chain.nodes.insert(chain.nodes.begin() + static_cast<ptrdiff_t>(ni), n);
                    auto& cn = get_counts(block_id).nodes;
                    cn.insert(cn.begin() + static_cast<ptrdiff_t>(ni), NodeCounts{passing, {}});
                }
                chain_write(block_id, std::move(chain));
                done = true;
                break;
            }

            uint8_t kd = key_nibble(key, key_pos + i);
            uint8_t cd = chain.path[i];
            if (kd == cd) {
                if (has_node) { stack.push_back({block_id, ni, -1}); ni++; }
                continue;
            }

            if (has_node) {
                ChainNode& node = chain.nodes[ni];
                int li = node.light_index(kd);
                if (li >= 0) {
                    stack.push_back({block_id, ni, kd});
                    block_id = node.lights[li].block;
                    key_pos  = key_pos + i + 1;
                    next     = true;
                    break;
                }
                // New light child at an existing node.
                uint64_t nb = chain_alloc(chain_from_key(key, key_pos + i + 1, record), block_id);
                init_counts(nb, 1, 0);
                size_t at = insert_light(node, {kd, 0, nb});
                auto& lc = get_counts(block_id).nodes[ni].lights;
                lc.insert(lc.begin() + static_cast<ptrdiff_t>(at), 1);
            } else {
                // New branch node at i, with the key's rest as its first light child.
                uint32_t passing = ni > 0 ? get_counts(block_id).nodes[ni - 1].heavy
                                          : get_counts(block_id).total;
                uint64_t nb = chain_alloc(chain_from_key(key, key_pos + i + 1, record), block_id);
                init_counts(nb, 1, 0);
                ChainNode n;
                n.split        = static_cast<uint16_t>(i);
                n.heavy_weight = floor_log2(passing);
                n.lights.push_back({kd, 0, nb});
                chain.nodes.insert(chain.nodes.begin() + static_cast<ptrdiff_t>(ni), n);
                auto& cn = get_counts(block_id).nodes;
                cn.insert(cn.begin() + static_cast<ptrdiff_t>(ni), NodeCounts{passing, {1}});
            }
            chain_write(block_id, std::move(chain));
            done = true;
        }

        if (next) continue;
        if (!done) {
            // Every nibble of the chain matched.
            if (key_pos + chain.path_len == kn) {
                if (chain.tail_record.valid()) return 0;  // duplicate
                chain.tail_record = record;
            } else {
                // The chain's path is a prefix of key: extend it. The old tail
                // key (if any) moves onto a new node; the new key continues past it.
                if (extend_chain(chain, key, key_pos + chain.path_len, record))
                    get_counts(block_id).nodes.push_back(NodeCounts{1, {}});
            }
            chain_write(block_id, std::move(chain));
        }
        break;
    }

    bloom_add(key);
    for (uint64_t b : visited) get_counts(b).total++;
    adjust_weights(stack, true);
    return total_flips_ - flips_before;
}

bool DiskTrie::insert_one_no_rebalance(DiskTrie& trie, DiskManager& dm,
                                        const std::string& key, RecordPtr record) {
    const size_t kn = key_nibbles(key);
    if (dm.root_block() == NULL_BLOCK) {
        uint64_t root = trie.chain_alloc(chain_from_key(key, 0, record));
        dm.set_root_block(root);
        return true;
    }

    uint64_t block_id = dm.root_block();
    size_t   key_pos  = 0;

    while (true) {
        ChainData chain = trie.chain_read(block_id);
        size_t    ni    = 0;
        bool      next  = false;

        for (size_t i = 0; i < chain.path_len; i++) {
            bool has_node = ni < chain.nodes.size() && chain.nodes[ni].split == i;

            if (key_pos + i == kn) {
                if (has_node) {
                    if (chain.nodes[ni].record.valid()) return false;
                    chain.nodes[ni].record = record;
                } else {
                    ChainNode n;
                    n.split  = static_cast<uint16_t>(i);
                    n.record = record;
                    chain.nodes.insert(chain.nodes.begin() + static_cast<ptrdiff_t>(ni), n);
                }
                trie.chain_write(block_id, std::move(chain));
                return true;
            }

            uint8_t kd = key_nibble(key, key_pos + i);
            if (kd == chain.path[i]) {
                if (has_node) ni++;
                continue;
            }
            if (has_node) {
                ChainNode& node = chain.nodes[ni];
                int li = node.light_index(kd);
                if (li >= 0) {
                    block_id = node.lights[li].block;
                    key_pos  = key_pos + i + 1;
                    next     = true;
                    break;
                }
                uint64_t nb = trie.chain_alloc(chain_from_key(key, key_pos + i + 1, record), block_id);
                insert_light(node, {kd, 0, nb});
            } else {
                uint64_t nb = trie.chain_alloc(chain_from_key(key, key_pos + i + 1, record), block_id);
                ChainNode n;
                n.split = static_cast<uint16_t>(i);
                n.lights.push_back({kd, 0, nb});
                chain.nodes.insert(chain.nodes.begin() + static_cast<ptrdiff_t>(ni), n);
            }
            trie.chain_write(block_id, std::move(chain));
            return true;
        }

        if (next) continue;

        if (key_pos + chain.path_len == kn) {
            if (chain.tail_record.valid()) return false;
            chain.tail_record = record;
        } else {
            extend_chain(chain, key, key_pos + chain.path_len, record);
        }
        trie.chain_write(block_id, std::move(chain));
        return true;
    }
}

void DiskTrie::bulk_insert(std::vector<std::pair<std::string, RecordPtr>> kvs) {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    // Write back dirty cached chains before dropping the cache: they can hold
    // structural changes (e.g. a remove's unlinked child) not yet on disk.
    hot_flush_all();
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
    counts_ready_ = true;

    // Now fix up the on-disk weights to match the rebuilt counts.
    if (dm_.root_block() == NULL_BLOCK) return;
    std::vector<uint64_t> stack = {dm_.root_block()};
    while (!stack.empty()) {
        uint64_t bid = stack.back(); stack.pop_back();
        ChainData chain = chain_read(bid);
        ChainCounts& cc = counts_[bid];
        bool changed = false;
        for (size_t ni = 0; ni < chain.nodes.size(); ni++) {
            ChainNode& node = chain.nodes[ni];
            uint16_t hw = floor_log2(cc.nodes[ni].heavy);
            if (node.heavy_weight != hw) { node.heavy_weight = hw; changed = true; }
            for (size_t k = 0; k < node.lights.size(); k++) {
                uint16_t lw = floor_log2(cc.nodes[ni].lights[k]);
                if (node.lights[k].weight != lw) { node.lights[k].weight = lw; changed = true; }
                stack.push_back(node.lights[k].block);
            }
        }
        if (changed) chain_write(bid, std::move(chain));
    }
    hot_flush_all();
}

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
    // Pass 2: rewrite each new slot with updated light-child pointers.
    // Remapping only changes fixed-width pointers, so every chain fits in place.
    for (auto& [old_addr, new_addr] : remap) {
        ChainData chain = dm_.read_chain_at(old_addr);
        for (auto& node : chain.nodes)
            for (auto& l : node.lights) l.block = remap.at(l.block);
        dm_.update_chain_at(new_addr, chain);
    }

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
    for (auto& node : chain.nodes)
        for (auto& l : node.lights)
            if (l.digit < chain.path[node.split])
                compact_assign_lex(l.block, last_phys, remap);

    // Allocate this chain adjacent to its lex-predecessor.
    uint64_t new_addr = dm_.alloc_chain_slot(chain, last_phys);
    remap[old_addr]   = new_addr;
    last_phys         = chain_addr_phys(new_addr);

    for (auto& node : chain.nodes)
        for (auto& l : node.lights)
            if (l.digit > chain.path[node.split])
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

bool DiskTrie::remove(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    ensure_counts();
    if (dm_.root_block() == NULL_BLOCK) return false;
    const size_t kn = key_nibbles(key);

    std::vector<Frame>    stack;
    std::vector<uint64_t> visited;
    uint64_t block_id = dm_.root_block();
    size_t   key_pos  = 0;

    while (true) {
        ChainData chain = chain_read(block_id);
        visited.push_back(block_id);
        size_t ni   = 0;
        bool   next = false;

        for (size_t i = 0; i < chain.path_len; i++) {
            bool has_node = ni < chain.nodes.size() && chain.nodes[ni].split == i;

            if (key_pos + i == kn) {
                if (!has_node || !chain.nodes[ni].record.valid()) return false;
                chain.nodes[ni].record = RecordPtr{};
                chain_write(block_id, std::move(chain));
                goto removed;
            }

            uint8_t kd = key_nibble(key, key_pos + i);
            if (kd == chain.path[i]) {
                if (has_node) { stack.push_back({block_id, ni, -1}); ni++; }
                continue;
            }
            if (!has_node) return false;
            int li = chain.nodes[ni].light_index(kd);
            if (li < 0) return false;
            stack.push_back({block_id, ni, kd});
            block_id = chain.nodes[ni].lights[li].block;
            key_pos  = key_pos + i + 1;
            next     = true;
            break;
        }

        if (next) continue;

        if (key_pos + chain.path_len != kn || !chain.tail_record.valid()) return false;
        chain.tail_record = RecordPtr{};
        chain_write(block_id, std::move(chain));
        break;
    }

removed:
    for (uint64_t b : visited) {
        auto& t = get_counts(b).total;
        if (t > 0) t--;
    }
    adjust_weights(stack, false);
    return true;
}

bool DiskTrie::lookup(const std::string& key, RecordPtr* ptr_out,
                      size_t* chains_out) const {
    std::shared_lock<std::shared_mutex> lock(trie_latch_);  // inserts write bloom_ under the exclusive latch
    if (!bloom_may_contain(key)) return false;
    if (dm_.root_block() == NULL_BLOCK) return false;

    size_t   chains  = 1;
    uint64_t cur     = dm_.root_block();
    size_t   key_pos = 0;

    while (cur != NULL_BLOCK) {
        // Match each chain where it lives instead of decoding a ChainData per
        // hop: in the hot cache (it may be newer than disk), else the mapped bytes.
        MatchOutcome r;
        bool cached = false;
        {
            std::lock_guard<std::mutex> lk(hot_mu_);
            auto it = hot_.find(cur);
            if (it != hot_.end()) {
                r = chain_match(it->second.data, key, key_pos);
                cached = true;
            }
        }
        if (!cached) {
            DiskManager::ChainBytes cb = dm_.pin_chain_bytes(cur);
            bool ok;
            r = chain_match_raw(cb.data, cb.len, key, key_pos, ok);
            dm_.unpin_chain_shared(cb.pinned_block);
            if (!ok) throw std::runtime_error("lookup: invalid chain bytes");
        }

        switch (r.kind) {
            case MatchOutcome::Kind::FOUND:
                if (ptr_out)    *ptr_out    = r.record;
                if (chains_out) *chains_out = chains;
                return true;
            case MatchOutcome::Kind::MISS:
                return false;
            case MatchOutcome::Kind::FOLLOW_LIGHT:
                cur     = r.light_child;
                key_pos = r.next_key_pos;
                chains++;
                break;
        }
    }
    return false;
}
