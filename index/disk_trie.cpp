#include "index/disk_trie.h"
#include "index/disk_trie_internal.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>
#include <shared_mutex>
#include <stdexcept>

// The trie is base 16: keys are matched one nibble at a time (see chain.h).
// A chain is a heavy path; each node on it branches into up to 15 light
// children, and a lookup crosses O(log n) chains because a light child holds
// at most half of its parent's keys.

// ---- Bloom filter (index/bloom.h) ----

void DiskTrie::bloom_add(const std::string& key) {
    if (bloom_.empty()) bloom_.reserve(dm_.key_count());
    bloom_.add(key);
}

void DiskTrie::rebuild_bloom() {
    bloom_.clear();
    bloom_.reserve(dm_.key_count());
    TrieCursor  cursor;
    std::string key;
    RecordPtr   rec;
    if (!lower_bound("", cursor)) return;
    while (cursor_next(cursor, key, rec)) bloom_add(key);
}

// The sidecar's key count must match the trie header, which rejects a filter
// saved before later writes (e.g. a sidecar copied back or left by an older
// close).
bool DiskTrie::load_bloom() {
    return !bloom_path_.empty() && bloom_.load(bloom_path_, dm_.key_count());
}

void DiskTrie::save_bloom() {
    if (!bloom_path_.empty()) bloom_.save(bloom_path_, dm_.key_count());
}

bool DiskTrie::bloom_may_contain(const std::string& key) const {
    return bloom_.may_contain(key);
}

// ---- chain construction helpers ----

uint16_t DiskTrie::floor_log2(uint32_t n) {
    if (n <= 1) return 0;
    uint16_t k = 0;
    while ((1u << (k + 1)) <= n) ++k;
    return k;
}

bool DiskTrie::flip(uint64_t block_id, ChainData& chain, size_t ni, size_t li) {
    ChainNode&   node     = chain.nodes[ni];
    if (node.split >= chain.path_len) return false;  // end node: there is no heavy side
    size_t       s        = node.split;
    uint8_t      h        = chain.path[s];
    LightChild   light    = node.lights[li];
    ChainData    L        = chain_read(light.block);
    if (L.bucket) return false;  // buckets are leaves: a lookup ends there anyway
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
    return true;
}

// ---- insert / remove ----

void DiskTrie::adjust_weights(const std::vector<Frame>& stack, bool increment) {
    for (int f = static_cast<int>(stack.size()) - 1; f >= 0; f--) {
        uint64_t bid = stack[f].block_id;
        size_t   ni  = stack[f].ni;

        // Counts live in memory; the chain itself is read only when its stored
        // weight changes or a flip or merge is due, which is rare (#60).
        ChainCounts& cc = get_counts(bid);
        if (ni >= cc.nodes.size()) continue;
        NodeCounts&  nc = cc.nodes[ni];
        int li = -1;
        if (stack[f].light_digit >= 0) {
            li = stack[f].light_rank;
            if (li < 0 || static_cast<size_t>(li) >= nc.lights.size()) continue;  // child gone (cannot happen)
        }
        uint32_t& side = li < 0 ? nc.heavy : nc.lights[li];
        uint16_t  old_w = floor_log2(side);
        if (increment) side++;
        else if (side > 0) side--;
        uint16_t new_w = floor_log2(side);

        bool flip_light = increment && li >= 0 && nc.lights[li] > nc.heavy;
        size_t best = 0;
        for (size_t k = 1; k < nc.lights.size(); k++)
            if (nc.lights[k] > nc.lights[best]) best = k;
        bool flip_heavy = !increment && li < 0 && !nc.lights.empty() && nc.lights[best] > nc.heavy;
        bool merge      = !increment && li >= 0 && nc.lights[li] <= opts_.bucket_merge_keys;
        if (old_w == new_w && !flip_light && !flip_heavy && !merge) continue;

        ChainData chain = chain_read(bid);
        if (chain.bucket || ni >= chain.nodes.size()) continue;
        ChainNode& node = chain.nodes[ni];
        assert(li < 0 || node.light_index(static_cast<uint8_t>(stack[f].light_digit)) == li);
        uint16_t& stored = li < 0 ? node.heavy_weight : node.lights[li].weight;
        if (stored != new_w) {
            stored = new_w;
            chain_write(bid, chain);
        }

        // Flip when a light child now holds more keys than the heavy side:
        // after an insert into that light child, or a remove from the heavy side.
        // (flip refuses end nodes and bucket children.)
        if (flip_light) {
            flip(bid, chain, ni, static_cast<size_t>(li));
        } else if (flip_heavy) {
            flip(bid, chain, ni, best);
        } else if (merge) {
            // A light subtree shrank: once small, collapse it back into a bucket.
            merge_to_bucket(node.lights[li].block, stack[f].child_pos, bid, ni, li);
        }
    }
}

size_t DiskTrie::insert(const std::string& key, RecordPtr record) {
    return insert(key, [record](RecordPtr, RecordPtr) { return record; });
}

size_t DiskTrie::insert(const std::string& key, const RecordFn& make_record) {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    ensure_counts();
    return insert_locked(key, make_record);
}

// ---- buckets edited in place (#73) ----
// Buckets are never in the hot cache, so a cached address is a chain.

bool DiskTrie::is_cached(uint64_t addr) const {
    std::lock_guard<std::mutex> lk(hot_mu_);
    return hot_.count(addr) != 0;
}

int DiskTrie::insert_in_bucket(uint64_t addr, const std::string& key, size_t key_pos,
                               const std::function<RecordPtr(RecordPtr, RecordPtr)>& take) {
    if (is_cached(addr)) return 0;
    std::vector<uint8_t> suffix;
    size_t    idx;
    RecordPtr before, after;
    {
        DiskManager::ChainBytes cb = dm_.pin_chain_bytes(addr);
        if (!chain_bytes_are_bucket(cb.data, cb.len)) { dm_.unpin_chain_shared(cb.pinned_block); return 0; }
        suffix = key_suffix(key, key_pos);
        idx = bucket_lower_bound_raw(cb.data, suffix);
        if (bucket_entry_equals_raw(cb.data, idx, suffix)) { dm_.unpin_chain_shared(cb.pinned_block); return -1; }
        size_t n    = bucket_count_raw_bytes(cb.data);
        size_t need = bucket_size_raw(cb.data) + bucket_entry_bytes(suffix.size());
        before = idx > 0 ? bucket_record_raw(cb.data, idx - 1) : RecordPtr{};
        after  = idx < n ? bucket_record_raw(cb.data, idx)     : RecordPtr{};
        dm_.unpin_chain_shared(cb.pinned_block);
        // Moving to a larger slot or bursting is left to the decoding path.
        if (need > cb.len || need > opts_.bucket_max_bytes) return 0;
    }
    RecordPtr record = take(before, after);   // outside the pin: it may do heap work
    DiskManager::ChainSlot sl = dm_.pin_chain_slot(addr);
    bucket_insert_raw(sl.data, idx, suffix, record);
    dm_.unpin_chain_exclusive(sl.pinned_block);
    return 1;
}

int DiskTrie::erase_in_bucket(uint64_t addr, const std::string& key, size_t key_pos) {
    if (is_cached(addr)) return 0;
    DiskManager::ChainSlot sl = dm_.pin_chain_slot(addr);
    int r = 0;
    if (chain_bytes_are_bucket(sl.data, sl.capacity)) {
        std::vector<uint8_t> suffix = key_suffix(key, key_pos);
        size_t idx = bucket_lower_bound_raw(sl.data, suffix);
        if (bucket_entry_equals_raw(sl.data, idx, suffix)) { bucket_erase_raw(sl.data, idx); r = 1; }
        else r = -1;
    }
    dm_.unpin_chain_exclusive(sl.pinned_block);
    return r;
}

int DiskTrie::set_in_bucket(uint64_t addr, const std::string& key, size_t key_pos, RecordPtr record) {
    if (is_cached(addr)) return 0;
    DiskManager::ChainSlot sl = dm_.pin_chain_slot(addr);
    int r = 0;
    if (chain_bytes_are_bucket(sl.data, sl.capacity)) {
        std::vector<uint8_t> suffix = key_suffix(key, key_pos);
        size_t idx = bucket_lower_bound_raw(sl.data, suffix);
        if (bucket_entry_equals_raw(sl.data, idx, suffix)) { bucket_set_record_raw(sl.data, idx, record); r = 1; }
        else r = -1;
    }
    dm_.unpin_chain_exclusive(sl.pinned_block);
    return r;
}

size_t DiskTrie::insert_locked(const std::string& key, const RecordFn& make_record) {
    size_t flips_before = total_flips_;
    // The key's record, made once its place is known (past every duplicate check).
    RecordPtr record;
    auto take = [&](RecordPtr before, RecordPtr after) { record = make_record(before, after); return record; };

    if (dm_.root_block() == NULL_BLOCK) {
        bloom_add(key);
        uint64_t root = chain_alloc(single_bucket(key, 0, take({}, {})));
        init_counts(root, 1, 0);
        dm_.set_root_block(root);
        return 0;
    }

    std::vector<Frame>    stack;
    std::vector<uint64_t> visited;   // chains whose totals grow if the insert succeeds
    Descent d = descend(key, stack, visited,
                        [&](uint64_t addr, size_t kp) { return insert_in_bucket(addr, key, kp, take); });
    if (d.end == Descent::End::IN_PLACE && d.in_place < 0) return 0;   // duplicate
    uint64_t   block_id = d.block_id;
    size_t     key_pos  = d.key_pos;
    ChainData& chain    = d.chain;

    if (d.end == Descent::End::BUCKET) {
        std::vector<uint8_t> suffix = key_suffix(key, key_pos);
        size_t idx = bucket_lower_bound(chain, suffix);
        if (bucket_entry_equals(chain, idx, suffix)) return 0;  // duplicate
        // Neighbour hints: the entries just before and after the new key.
        size_t n = bucket_size(chain);
        take(idx > 0 ? bucket_record(chain, idx - 1) : RecordPtr{},
             idx < n ? bucket_record(chain, idx) : RecordPtr{});
        if (chain.bucket_bytes.size() + bucket_entry_bytes(suffix.size()) <= opts_.bucket_max_bytes) {
            // Outgrew its slot (in-place inserts are handled above): move it
            // and repoint the parent, whose light frame is the last pushed.
            bucket_insert_at(chain, idx, suffix, record);
            uint64_t parent = NULL_BLOCK; size_t pni = 0; int pli = -1;
            if (!stack.empty()) { parent = stack.back().block_id; pni = stack.back().ni; pli = stack.back().light_rank; }
            visited.back() = move_bucket(block_id, chain, parent, pni, pli);
        } else {
            std::vector<BucketEntry> entries;
            bucket_decode(chain, entries);
            entries.insert(entries.begin() + static_cast<ptrdiff_t>(idx), BucketEntry{std::move(suffix), record});
            // Burst: rebuild this subtree in place as a chain with bucket children.
            std::vector<BuildItem> items(entries.size());
            for (size_t k = 0; k < entries.size(); k++)
                items[k] = {std::move(entries[k].suffix), entries[k].record};
            ChainCounts cc;
            ChainData root = build_node(items, 0, items.size(), 0, key_pos, block_id, cc);
            chain_write(block_id, std::move(root));
            counts_[block_id] = std::move(cc);   // already includes the new key
            visited.pop_back();
            bucket_bursts_++;
        }
    } else if (d.end == Descent::End::CHAIN) {
        const ChainStop& at = d.stop;
        const size_t     ni = at.ni;
        // Keys continuing on the chain at the stop = those past the previous node.
        auto passing = [&] { return ni > 0 ? get_counts(block_id).nodes[ni - 1].heavy : get_counts(block_id).total; };
        switch (at.kind) {
        case ChainStop::Kind::ENDS:   // the key ends inside this chain
            if (at.has_node) {
                if (chain.nodes[ni].record.valid()) return 0;  // duplicate
                chain.nodes[ni].record = take({}, {});
            } else {
                uint32_t p = passing();
                ChainNode n;
                n.split        = static_cast<uint16_t>(at.pos);
                n.heavy_weight = floor_log2(p);
                n.record       = take({}, {});
                chain.nodes.insert(chain.nodes.begin() + static_cast<ptrdiff_t>(ni), n);
                auto& cn = get_counts(block_id).nodes;
                cn.insert(cn.begin() + static_cast<ptrdiff_t>(ni), NodeCounts{p, {}});
            }
            break;
        case ChainStop::Kind::DIVERGES:
            if (at.has_node) {
                // New light child at an existing node: a one-key bucket.
                uint64_t nb = chain_alloc(single_bucket(key, key_pos + at.pos + 1, take({}, {})), block_id);
                init_counts(nb, 1, 0);
                size_t li = insert_light(chain.nodes[ni], {at.digit, 0, nb});
                auto& lc = get_counts(block_id).nodes[ni].lights;
                lc.insert(lc.begin() + static_cast<ptrdiff_t>(li), 1);
            } else {
                // New branch node, with a one-key bucket as its first light child.
                uint32_t p  = passing();
                uint64_t nb = chain_alloc(single_bucket(key, key_pos + at.pos + 1, take({}, {})), block_id);
                init_counts(nb, 1, 0);
                ChainNode n;
                n.split        = static_cast<uint16_t>(at.pos);
                n.heavy_weight = floor_log2(p);
                n.lights.push_back({at.digit, 0, nb});
                chain.nodes.insert(chain.nodes.begin() + static_cast<ptrdiff_t>(ni), n);
                auto& cn = get_counts(block_id).nodes;
                cn.insert(cn.begin() + static_cast<ptrdiff_t>(ni), NodeCounts{p, {1}});
            }
            break;
        case ChainStop::Kind::TAIL:
            if (chain.tail_record.valid()) return 0;  // duplicate
            chain.tail_record = take({}, {});
            break;
        case ChainStop::Kind::PAST_END: {
            const size_t cpos = key_pos + chain.path_len;   // key nibble choosing a child
            uint64_t nb = chain_alloc(single_bucket(key, cpos + 1, take({}, {})), block_id);
            init_counts(nb, 1, 0);
            if (at.has_node) {
                size_t li = insert_light(chain.nodes[ni], {at.digit, 0, nb});
                auto& lc = get_counts(block_id).nodes[ni].lights;
                lc.insert(lc.begin() + static_cast<ptrdiff_t>(li), 1);
            } else {
                // The chain's path is a prefix of key: end the chain in a fan-out
                // node whose first child is a bucket holding the rest of key.
                ChainNode n;
                n.split = chain.path_len;
                n.lights.push_back({at.digit, 0, nb});
                chain.nodes.push_back(n);
                get_counts(block_id).nodes.push_back(NodeCounts{0, {1}});
            }
            break;
        }
        case ChainStop::Kind::FOLLOW:
            break;   // descend never stops on FOLLOW
        }
        chain_write(block_id, std::move(chain));
    }

    bloom_add(key);
    for (uint64_t b : visited) get_counts(b).total++;
    adjust_weights(stack, true);
    return total_flips_ - flips_before;
}

// After a remove (#38): unlink and free light subtrees left with no keys,
// drop nodes left with neither a record nor a light child, and carry on up
// while whole chains have emptied. Frames for what is pruned are popped, so
// adjust_weights only sees what remains.
void DiskTrie::prune_empty(std::vector<Frame>& stack) {
    while (!stack.empty() && stack.back().light_digit >= 0) {
        const Frame f = stack.back();
        ChainData parent = chain_read(f.block_id);
        if (f.ni >= parent.nodes.size() || f.light_rank < 0 ||
            static_cast<size_t>(f.light_rank) >= parent.nodes[f.ni].lights.size()) break;
        const size_t li    = static_cast<size_t>(f.light_rank);
        uint64_t     child = parent.nodes[f.ni].lights[li].block;
        if (get_counts(child).total != 0) break;

        std::vector<BucketEntry> none;
        std::vector<uint64_t>    addrs;
        collect_subtree(child, {}, none, addrs);
        for (uint64_t a : addrs) {
            hot_discard(a);
            counts_.erase(a);
            dm_.free_chain_slot(a);
        }
        ChainNode&   node = parent.nodes[f.ni];
        ChainCounts& pc   = get_counts(f.block_id);
        node.lights.erase(node.lights.begin() + static_cast<ptrdiff_t>(li));
        if (f.ni < pc.nodes.size() && li < pc.nodes[f.ni].lights.size())
            pc.nodes[f.ni].lights.erase(pc.nodes[f.ni].lights.begin() + static_cast<ptrdiff_t>(li));
        if (!node.record.valid() && node.lights.empty()) {
            parent.nodes.erase(parent.nodes.begin() + static_cast<ptrdiff_t>(f.ni));
            if (f.ni < pc.nodes.size()) pc.nodes.erase(pc.nodes.begin() + static_cast<ptrdiff_t>(f.ni));
        }
        chain_write(f.block_id, std::move(parent));
        stack.pop_back();
        pruned_++;

        // If this whole chain is now empty, its own heavy frames go too and its
        // parent prunes it next.
        if (pc.total != 0) break;
        while (!stack.empty() && stack.back().block_id == f.block_id && stack.back().light_digit < 0)
            stack.pop_back();
    }
}

void DiskTrie::repoint_child(uint64_t parent, size_t ni, int li, uint64_t old_addr, uint64_t new_addr) {
    if (parent == NULL_BLOCK) {
        assert(dm_.root_block() == old_addr);
        dm_.set_root_block(new_addr);
    } else {
        ChainData p = chain_read(parent);
        assert(ni < p.nodes.size() && li >= 0 && static_cast<size_t>(li) < p.nodes[ni].lights.size());
        assert(p.nodes[ni].lights[static_cast<size_t>(li)].block == old_addr);
        p.nodes[ni].lights[static_cast<size_t>(li)].block = new_addr;
        chain_write(parent, std::move(p));
    }
    auto it = counts_.find(old_addr);
    if (it != counts_.end()) {
        ChainCounts cc = std::move(it->second);
        counts_.erase(it);
        counts_[new_addr] = std::move(cc);
    }
}

uint64_t DiskTrie::move_bucket(uint64_t addr, const ChainData& bucket, uint64_t parent, size_t ni, int li) {
    uint64_t moved = dm_.alloc_chain_slot(bucket, chain_addr_phys(addr));
    hot_discard(addr);
    dm_.free_chain_slot(addr);
    repoint_child(parent, ni, li, addr, moved);
    return moved;
}

bool DiskTrie::remove(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    ensure_counts();
    if (dm_.root_block() == NULL_BLOCK) return false;

    std::vector<Frame>    stack;
    std::vector<uint64_t> visited;
    Descent d = descend(key, stack, visited,
                        [&](uint64_t addr, size_t kp) { return erase_in_bucket(addr, key, kp); });
    if (d.end == Descent::End::IN_PLACE && d.in_place < 0) return false;   // not in the bucket
    ChainData& chain = d.chain;
    if (d.end == Descent::End::BUCKET) {
        std::vector<uint8_t> suffix = key_suffix(key, d.key_pos);
        size_t idx = bucket_lower_bound(chain, suffix);
        if (!bucket_entry_equals(chain, idx, suffix)) return false;
        bucket_erase_at(chain, idx);
        chain_write(d.block_id, std::move(chain));
    } else if (d.end == Descent::End::CHAIN) {
        const ChainStop& at = d.stop;
        if (at.kind == ChainStop::Kind::ENDS) {
            if (!at.has_node || !chain.nodes[at.ni].record.valid()) return false;
            chain.nodes[at.ni].record = RecordPtr{};
            if (chain.nodes[at.ni].lights.empty()) {   // nothing left at this node (#38)
                chain.nodes.erase(chain.nodes.begin() + static_cast<ptrdiff_t>(at.ni));
                auto& cn = get_counts(d.block_id).nodes;
                if (at.ni < cn.size()) cn.erase(cn.begin() + static_cast<ptrdiff_t>(at.ni));
            }
        } else if (at.kind == ChainStop::Kind::TAIL) {
            if (!chain.tail_record.valid()) return false;
            chain.tail_record = RecordPtr{};
        } else {
            return false;   // diverges from the trie: not stored
        }
        chain_write(d.block_id, std::move(chain));
    }

    for (uint64_t b : visited) {
        auto& t = get_counts(b).total;
        if (t > 0) t--;
    }
    prune_empty(stack);
    adjust_weights(stack, false);

    // The whole trie may now be small enough to be a single bucket again.
    uint64_t root = dm_.root_block();
    if (get_counts(root).total <= opts_.bucket_merge_keys) merge_to_bucket(root, 0, NULL_BLOCK, 0, -1);
    return true;
}

bool DiskTrie::set_record(const std::string& key, RecordPtr record) {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    return set_record_locked(key, record);
}

// True if a and b agree on their first n nibbles (both have at least n).
static bool same_nibble_prefix(const std::string& a, const std::string& b, size_t n) {
    if (key_nibbles(a) < n || key_nibbles(b) < n) return false;
    if (memcmp(a.data(), b.data(), n / 2) != 0) return false;
    return n % 2 == 0 || key_nibble(a, n - 1) == key_nibble(b, n - 1);
}

size_t DiskTrie::set_records(std::vector<std::pair<std::string, RecordPtr>> kvs) {
    std::sort(kvs.begin(), kvs.end(),
              [](const auto& x, const auto& y) { return x.first < y.first; });
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    if (dm_.root_block() == NULL_BLOCK) return 0;

    size_t found = 0;
    for (size_t i = 0; i < kvs.size();) {
        // Descend for kvs[i]; a bucket also holds every later key sharing the
        // nibbles that led to it, since it is that prefix's whole subtree.
        const std::string& key = kvs[i].first;
        uint64_t block_id = dm_.root_block();
        size_t   key_pos  = 0;
        ChainData chain;
        while (true) {
            chain = chain_read(block_id);
            if (chain.bucket) break;
            MatchOutcome m = chain_match(chain, key, key_pos);
            if (m.kind != MatchOutcome::Kind::FOLLOW_LIGHT) break;
            block_id = m.light_child;
            key_pos  = m.next_key_pos;
        }
        if (!chain.bucket) {
            found += set_record_locked(key, kvs[i].second);
            i++;
            continue;
        }
        size_t j = i;
        std::vector<size_t> fallback;
        for (; j < kvs.size() && same_nibble_prefix(kvs[j].first, key, key_pos); j++) {
            std::vector<uint8_t> suffix = key_suffix(kvs[j].first, key_pos);
            size_t idx = bucket_lower_bound(chain, suffix);
            if (bucket_entry_equals(chain, idx, suffix)) {
                bucket_set_record(chain, idx, kvs[j].second);
                found++;
            } else {
                fallback.push_back(j);
            }
        }
        if (j == i) {   // key shorter than the bucket's prefix: not in it
            found += set_record_locked(key, kvs[i].second);
            i++;
            continue;
        }
        chain_write(block_id, std::move(chain));
        for (size_t f : fallback) found += set_record_locked(kvs[f].first, kvs[f].second);
        i = j;
    }
    return found;
}

bool DiskTrie::set_record_locked(const std::string& key, RecordPtr record) {
    if (dm_.root_block() == NULL_BLOCK) return false;
    std::vector<Frame>    stack;     // not needed here; descend records them anyway
    std::vector<uint64_t> visited;
    Descent d = descend(key, stack, visited,
                        [&](uint64_t addr, size_t kp) { return set_in_bucket(addr, key, kp, record); });
    if (d.end == Descent::End::IN_PLACE) return d.in_place > 0;
    ChainData& chain = d.chain;
    if (d.end == Descent::End::BUCKET) {
        std::vector<uint8_t> suffix = key_suffix(key, d.key_pos);
        size_t idx = bucket_lower_bound(chain, suffix);
        if (!bucket_entry_equals(chain, idx, suffix)) return false;
        bucket_set_record(chain, idx, record);
    } else {
        // Found on this chain: on the node where the key ends, or at its tail.
        const ChainStop& at = d.stop;
        if (at.kind == ChainStop::Kind::ENDS && at.has_node && chain.nodes[at.ni].record.valid())
            chain.nodes[at.ni].record = record;
        else if (at.kind == ChainStop::Kind::TAIL && chain.tail_record.valid())
            chain.tail_record = record;
        else
            return false;
    }
    chain_write(d.block_id, std::move(chain));
    return true;
}

// ---- the shared descent (#89) ----

DiskTrie::ChainStop DiskTrie::scan_chain(uint64_t block_id, const ChainData& chain, const std::string& key,
                                         size_t key_pos, std::vector<Frame>& stack) const {
    using K = ChainStop::Kind;
    const size_t kn = key_nibbles(key);
    size_t ni = 0;
    for (size_t i = 0; i < chain.path_len; i++) {
        const bool has_node = ni < chain.nodes.size() && chain.nodes[ni].split == i;
        if (key_pos + i == kn) return {K::ENDS, i, ni, 0, has_node};
        const uint8_t kd = key_nibble(key, key_pos + i);
        if (kd == chain.path[i]) {
            if (has_node) { stack.push_back({block_id, ni, -1}); ni++; }
            continue;
        }
        if (has_node) {
            int li = chain.nodes[ni].light_index(kd);
            if (li >= 0) {
                stack.push_back({block_id, ni, kd, key_pos + i + 1, li});
                return {K::FOLLOW, i, ni, key_pos + i + 1, true, kd, chain.nodes[ni].lights[li].block};
            }
        }
        return {K::DIVERGES, i, ni, 0, has_node, kd};
    }
    if (key_pos + chain.path_len == kn) return {K::TAIL, chain.path_len, ni};
    // Past the chain's end: the next nibble picks an end-node child.
    const size_t  cpos = key_pos + chain.path_len;
    const uint8_t kd   = key_nibble(key, cpos);
    if (!chain.has_end_node()) return {K::PAST_END, chain.path_len, chain.nodes.size(), 0, false, kd};
    const size_t ei = chain.nodes.size() - 1;
    int li = chain.nodes[ei].light_index(kd);
    if (li < 0) return {K::PAST_END, chain.path_len, ei, 0, true, kd};
    stack.push_back({block_id, ei, kd, cpos + 1, li});
    return {K::FOLLOW, chain.path_len, ei, cpos + 1, true, kd, chain.nodes[ei].lights[li].block};
}

DiskTrie::Descent DiskTrie::descend(const std::string& key, std::vector<Frame>& stack,
                                    std::vector<uint64_t>& visited,
                                    const std::function<int(uint64_t, size_t)>& in_bucket) {
    Descent d;
    d.block_id = dm_.root_block();
    while (true) {
        if (pass_through(d.block_id, d.key_pos, key, stack, visited)) continue;
        if ((d.in_place = in_bucket(d.block_id, d.key_pos)) != 0) {
            if (d.in_place > 0) visited.push_back(d.block_id);
            d.end = Descent::End::IN_PLACE;
            return d;
        }
        d.chain = chain_read(d.block_id);
        visited.push_back(d.block_id);
        if (d.chain.bucket) { d.end = Descent::End::BUCKET; return d; }
        d.stop = scan_chain(d.block_id, d.chain, key, d.key_pos, stack);
        if (d.stop.kind != ChainStop::Kind::FOLLOW) { d.end = Descent::End::CHAIN; return d; }
        d.block_id = d.stop.child;
        d.key_pos  = d.stop.next_pos;
    }
}

MatchOutcome DiskTrie::match_in_place(uint64_t addr, const std::string& key, size_t key_pos) const {
    {
        std::lock_guard<std::mutex> lk(hot_mu_);
        auto it = hot_.find(addr);
        if (it != hot_.end()) return chain_match(it->second.data, key, key_pos);
    }
    DiskManager::ChainBytes cb = dm_.pin_chain_bytes(addr);
    bool ok;
    MatchOutcome r = chain_match_raw(cb.data, cb.len, key, key_pos, ok);
    dm_.unpin_chain_shared(cb.pinned_block);
    if (!ok) throw std::runtime_error("match_in_place: invalid chain bytes");
    return r;
}

// Pass a chain the key only goes through (it follows a light child): record
// the frames the decoding walk would push, without decoding the chain (#60).
bool DiskTrie::pass_through(uint64_t& block_id, size_t& key_pos, const std::string& key,
                            std::vector<Frame>& stack, std::vector<uint64_t>& visited) {
    // A bucket is always where the walk ends: leave it to the decoding path
    // without searching it here first.
    MatchOutcome m;
    bool cached = false;
    {
        std::lock_guard<std::mutex> lk(hot_mu_);
        auto it = hot_.find(block_id);
        if (it != hot_.end()) {
            if (it->second.data.bucket) return false;
            m = chain_match(it->second.data, key, key_pos);
            cached = true;
        }
    }
    if (!cached) {
        DiskManager::ChainBytes cb = dm_.pin_chain_bytes(block_id);
        bool ok = true;
        bool bucket = chain_bytes_are_bucket(cb.data, cb.len);
        if (!bucket) m = chain_match_raw(cb.data, cb.len, key, key_pos, ok);
        dm_.unpin_chain_shared(cb.pinned_block);
        if (bucket) return false;
        if (!ok) throw std::runtime_error("pass_through: invalid chain bytes");
    }
    if (m.kind != MatchOutcome::Kind::FOLLOW_LIGHT) return false;
    visited.push_back(block_id);
    for (size_t n = 0; n < m.node_index; n++) stack.push_back({block_id, n, -1});
    stack.push_back({block_id, m.node_index, key_nibble(key, m.next_key_pos - 1), m.next_key_pos, m.light_rank});
    block_id = m.light_child;
    key_pos  = m.next_key_pos;
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
        MatchOutcome r = match_in_place(cur, key, key_pos);

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
