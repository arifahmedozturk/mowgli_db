#include "index/disk_trie.h"
#include <algorithm>
#include <cassert>
#include <future>
#include <shared_mutex>

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
    auto it = hot_.find(block_id);
    if (it != hot_.end()) {
        hot_touch(block_id);
        return it->second.data;
    }
    if (hot_.size() >= HOT_CAPACITY)
        hot_evict_one();
    ChainData data = dm_.read_chain(block_id);
    hot_[block_id] = HotEntry{data, 0};
    hot_lru_.push_front(block_id);
    hot_lru_pos_[block_id] = hot_lru_.begin();
    return data;
}

void DiskTrie::chain_write(uint64_t block_id, ChainData chain) {
    auto it = hot_.find(block_id);
    if (it != hot_.end()) {
        it->second.data  = std::move(chain);
        it->second.dirty++;
        hot_touch(block_id);
        if (it->second.dirty >= DIRTY_FLUSH_AT) {
            dm_.update_chain(block_id, it->second.data);
            it->second.dirty = 0;
        }
    } else {
        if (hot_.size() >= HOT_CAPACITY)
            hot_evict_one();
        hot_[block_id] = HotEntry{std::move(chain), 1};
        hot_lru_.push_front(block_id);
        hot_lru_pos_[block_id] = hot_lru_.begin();
    }
}

uint64_t DiskTrie::chain_alloc(const ChainData& chain, uint64_t hint) {
    uint64_t hint_phys = (hint != NULL_BLOCK) ? chain_addr_phys(hint) : NULL_BLOCK;
    uint64_t id = dm_.alloc_chain_slot(chain, hint_phys);
    if (hot_.size() >= HOT_CAPACITY)
        hot_evict_one();
    hot_[id] = HotEntry{chain, 0};
    hot_lru_.push_front(id);
    hot_lru_pos_[id] = hot_lru_.begin();
    return id;
}

void DiskTrie::hot_touch(uint64_t block_id) const {
    auto pit = hot_lru_pos_.find(block_id);
    if (pit == hot_lru_pos_.end()) return;
    hot_lru_.erase(pit->second);
    hot_lru_.push_front(block_id);
    pit->second = hot_lru_.begin();
}

void DiskTrie::hot_evict_one() const {
    if (hot_lru_.empty()) return;
    uint64_t victim = hot_lru_.back();
    hot_lru_.pop_back();
    hot_lru_pos_.erase(victim);
    auto it = hot_.find(victim);
    if (it != hot_.end()) {
        if (it->second.dirty > 0)
            dm_.update_chain(victim, it->second.data);
        hot_.erase(it);
    }
}

void DiskTrie::hot_invalidate(uint64_t block_id) {
    auto it = hot_.find(block_id);
    if (it == hot_.end()) return;
    if (it->second.dirty > 0)
        dm_.update_chain(block_id, it->second.data);
    hot_.erase(it);
    auto pit = hot_lru_pos_.find(block_id);
    if (pit != hot_lru_pos_.end()) {
        hot_lru_.erase(pit->second);
        hot_lru_pos_.erase(pit);
    }
}

void DiskTrie::hot_discard(uint64_t block_id) {
    // Remove from hot cache WITHOUT flushing — used when a chain is being freed.
    auto it = hot_.find(block_id);
    if (it == hot_.end()) return;
    hot_.erase(it);
    auto pit = hot_lru_pos_.find(block_id);
    if (pit != hot_lru_pos_.end()) {
        hot_lru_.erase(pit->second);
        hot_lru_pos_.erase(pit);
    }
}

void DiskTrie::hot_flush_all() {
    for (auto& [id, entry] : hot_)
        if (entry.dirty > 0)
            dm_.update_chain(id, entry.data);
    // Clear so destructor-then-reuse doesn't double-flush.
    for (auto& [id, entry] : hot_)
        entry.dirty = 0;
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

using ScanResult = std::vector<std::pair<std::string, RecordPtr>>;

static constexpr uint16_t PARALLEL_WEIGHT_CUTOFF = 9; // floor(log2(512))

// Returns true when every key reachable under this prefix is strictly less
// than lo (i.e. prefix_max = prefix + 1...1 < lo).
static bool prefix_below_lo(const std::vector<bool>& prefix, const std::string& lo) {
    for (size_t i = 0; i < prefix.size(); i++) {
        bool lb = (static_cast<uint8_t>(lo[i / 8]) >> (7 - i % 8)) & 1;
        if (prefix[i] > lb) return false;
        if (prefix[i] < lb) return true;
    }
    return false;
}

// Returns true when every key reachable under this prefix is strictly greater
// than hi (i.e. prefix_min = prefix + 0...0 > hi).
static bool prefix_exceeds_hi(const std::vector<bool>& prefix, const std::string& hi) {
    for (size_t i = 0; i < prefix.size(); i++) {
        bool hb = (static_cast<uint8_t>(hi[i / 8]) >> (7 - i % 8)) & 1;
        if (prefix[i] > hb) return true;
        if (prefix[i] < hb) return false;
    }
    return false;
}

// Forward declarations — chain_traverse and trie_traverse are mutually recursive.
static ScanResult trie_traverse(const DiskTrie& trie, uint64_t block_id,
                                 std::vector<bool> prefix,
                                 const std::string& lo, const std::string& hi);

static ScanResult chain_traverse(const DiskTrie& trie, const ChainData& chain,
                                  size_t ni, size_t path_pos,
                                  std::vector<bool>& prefix,
                                  const std::string& lo, const std::string& hi) {
    ScanResult out;

    size_t next_split = (ni < chain.nodes.size())
                        ? chain.nodes[ni].split_bit
                        : chain.path_bit_len;

    for (size_t i = path_pos; i < next_split; i++)
        prefix.push_back(path_bit(chain, i));

    // Prune entire chain if the accumulated prefix is already out of range.
    if (prefix_exceeds_hi(prefix, hi) || prefix_below_lo(prefix, lo)) {
        for (size_t i = path_pos; i < next_split; i++) prefix.pop_back();
        return out;
    }

    if (ni >= chain.nodes.size()) {
        if (chain.tail_record.valid()) {
            std::string key = bits_to_key(prefix);
            if (key >= lo && key <= hi)
                out.push_back({key, chain.tail_record});
        }
    } else {
        const ChainNode& node = chain.nodes[ni];

        if (node.record.valid()) {
            std::string key = bits_to_key(prefix);
            if (key >= lo && key <= hi)
                out.push_back({key, node.record});
        }

        bool heavy_bit = path_bit(chain, node.split_bit);

        std::future<ScanResult> light_future;

        auto maybe_spawn_light = [&](int bit) -> std::future<ScanResult> {
            if (static_cast<bool>(bit) != heavy_bit &&
                node.light_child_block != NULL_BLOCK) {
                std::vector<bool> light_prefix = prefix;
                light_prefix.push_back(bit == 1);
                // Prune before spawning.
                if (prefix_exceeds_hi(light_prefix, hi) ||
                    prefix_below_lo(light_prefix, lo))
                    return {};
                uint64_t lb = node.light_child_block;
                if (node.light_child_weight >= PARALLEL_WEIGHT_CUTOFF) {
                    return std::async(std::launch::async,
                        [&trie, lb, lp = std::move(light_prefix), &lo, &hi]() mutable {
                            return trie_traverse(trie, lb, std::move(lp), lo, hi);
                        });
                } else {
                    auto r = trie_traverse(trie, lb, std::move(light_prefix), lo, hi);
                    std::promise<ScanResult> p;
                    p.set_value(std::move(r));
                    return p.get_future();
                }
            }
            return {};
        };

        for (int bit = 0; bit <= 1; bit++) {
            prefix.push_back(bit == 1);
            // Prune this branch if it can't contain any in-range keys.
            bool prune = prefix_exceeds_hi(prefix, hi) || prefix_below_lo(prefix, lo);
            if (!prune) {
                if (static_cast<bool>(bit) == heavy_bit) {
                    ScanResult heavy = chain_traverse(trie, chain, ni + 1,
                                                       node.split_bit + 1,
                                                       prefix, lo, hi);
                    out.insert(out.end(),
                               std::make_move_iterator(heavy.begin()),
                               std::make_move_iterator(heavy.end()));
                } else {
                    light_future = maybe_spawn_light(bit);
                }
            }
            prefix.pop_back();
        }

        if (light_future.valid()) {
            ScanResult light = light_future.get();
            if (!light.empty()) {
                ScanResult merged;
                merged.reserve(out.size() + light.size());
                std::merge(std::make_move_iterator(out.begin()),
                           std::make_move_iterator(out.end()),
                           std::make_move_iterator(light.begin()),
                           std::make_move_iterator(light.end()),
                           std::back_inserter(merged),
                           [](const auto& a, const auto& b){ return a.first < b.first; });
                out = std::move(merged);
            }
        }
    }

    for (size_t i = path_pos; i < next_split; i++)
        prefix.pop_back();

    return out;
}

static ScanResult trie_traverse(const DiskTrie& trie, uint64_t block_id,
                                 std::vector<bool> prefix,
                                 const std::string& lo, const std::string& hi) {
    if (prefix_exceeds_hi(prefix, hi) || prefix_below_lo(prefix, lo))
        return {};
    ChainData chain = trie.chain_read_shared(block_id);
    return chain_traverse(trie, chain, 0, 0, prefix, lo, hi);
}

void DiskTrie::range_scan(const std::string& lo, const std::string& hi,
                          std::vector<std::pair<std::string, RecordPtr>>& out) const {
    std::shared_lock<std::shared_mutex> lock(trie_latch_);
    if (dm_.root_block() == NULL_BLOCK) return;
    std::vector<bool> prefix;
    ScanResult result = trie_traverse(*this, dm_.root_block(), prefix, lo, hi);
    out = std::move(result);
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
    hot_lru_.clear();
    hot_lru_pos_.clear();

    for (auto& [key, ptr] : kvs)
        insert_one_no_rebalance(*this, dm_, key, ptr);

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

void DiskTrie::compact() {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    if (dm_.root_block() == NULL_BLOCK) return;

    hot_flush_all();
    hot_.clear(); hot_lru_.clear(); hot_lru_pos_.clear();

    // Prevent alloc_chain_slot from reusing old partially-filled blocks.
    // New slots must land in fresh blocks so old blocks can be fully recycled.
    dm_.clear_pack_candidates();

    uint64_t old_root = dm_.root_block();

    // Pass 1: DFS pre-order — allocate new slots, build old→new remap.
    // Each chain is co-located with its direct light children.
    std::unordered_map<uint64_t, uint64_t> remap;
    remap.reserve(counts_.size());
    compact_assign(old_root, NULL_BLOCK, remap);

    // Pass 2: rewrite each new slot with updated light_child_block pointers.
    for (auto& [old_addr, new_addr] : remap) {
        ChainData chain = dm_.read_chain_at(old_addr);
        for (auto& node : chain.nodes)
            if (node.light_child_block != NULL_BLOCK)
                node.light_child_block = remap.at(node.light_child_block);
        dm_.update_chain_at(new_addr, chain);
    }

    // Pass 3: free all old slots, update root.
    for (auto& [old_addr, new_addr] : remap)
        dm_.free_chain_slot(old_addr);
    dm_.set_root_block(remap.at(old_root));

    // counts_ keys are all stale — rebuild from new layout.
    counts_.clear();
    rebuild_counts();
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
