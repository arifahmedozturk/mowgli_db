#include "index/disk_trie.h"
#include "index/disk_trie_internal.h"
#include <functional>
#include <shared_mutex>

// Inserting a key: descend to where it stops, then one handler per place —
// a bucket (edited in place, moved to a bigger slot, or burst into a chain),
// the node where the key ends, a new light child, a new branch or end node,
// or the chain's tail.

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

size_t DiskTrie::insert_locked(const std::string& key, const RecordFn& make_record) {
    size_t flips_before = total_flips_;
    // Makes the key's record once its place is known (past every duplicate check).
    const RecordFn& take = make_record;

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
    if (d.end == Descent::End::BUCKET && !insert_in_decoded_bucket(key, d, stack, visited, take))
        return 0;                                                       // duplicate
    if (d.end == Descent::End::CHAIN && !insert_on_chain(key, d, take))
        return 0;                                                       // duplicate

    bloom_add(key);
    for (uint64_t b : visited) get_counts(b).total++;
    adjust_weights(stack, true);
    return total_flips_ - flips_before;
}

bool DiskTrie::insert_in_decoded_bucket(const std::string& key, Descent& d, const std::vector<Frame>& stack,
                                        std::vector<uint64_t>& visited, const RecordFn& take) {
    ChainData& chain = d.chain;
    std::vector<uint8_t> suffix = key_suffix(key, d.key_pos);
    size_t idx = bucket_lower_bound(chain, suffix);
    if (bucket_entry_equals(chain, idx, suffix)) return false;  // duplicate
    // Neighbour hints: the entries just before and after the new key.
    size_t n = bucket_size(chain);
    RecordPtr record = take(idx > 0 ? bucket_record(chain, idx - 1) : RecordPtr{},
                            idx < n ? bucket_record(chain, idx) : RecordPtr{});
    if (chain.bucket_bytes.size() + bucket_entry_bytes(suffix.size()) <= opts_.bucket_max_bytes) {
        // Outgrew its slot (in-place inserts are handled above): move it
        // and repoint the parent, whose light frame is the last pushed.
        bucket_insert_at(chain, idx, suffix, record);
        uint64_t parent = NULL_BLOCK; size_t pni = 0; int pli = -1;
        if (!stack.empty()) { parent = stack.back().block_id; pni = stack.back().ni; pli = stack.back().light_rank; }
        visited.back() = move_bucket(d.block_id, chain, parent, pni, pli);
    } else {
        burst_bucket(d.block_id, chain, idx, std::move(suffix), record, d.key_pos);
        visited.pop_back();   // the rebuilt counts already include the new key
    }
    return true;
}

void DiskTrie::burst_bucket(uint64_t block_id, const ChainData& bucket, size_t idx,
                            std::vector<uint8_t> suffix, RecordPtr record, size_t key_pos) {
    std::vector<BucketEntry> entries;
    bucket_decode(bucket, entries);
    entries.insert(entries.begin() + static_cast<ptrdiff_t>(idx), BucketEntry{std::move(suffix), record});
    // Burst: rebuild this subtree in place as a chain with bucket children.
    std::vector<BuildItem> items(entries.size());
    for (size_t k = 0; k < entries.size(); k++)
        items[k] = {std::move(entries[k].suffix), entries[k].record};
    ChainCounts cc;
    ChainData root = build_node(items, 0, items.size(), 0, key_pos, block_id, cc);
    chain_write(block_id, std::move(root));
    counts_[block_id] = std::move(cc);   // already includes the new key
    bucket_bursts_++;
}

bool DiskTrie::insert_on_chain(const std::string& key, Descent& d, const RecordFn& take) {
    const ChainStop& at = d.stop;
    ChainData& chain = d.chain;
    switch (at.kind) {
    case ChainStop::Kind::ENDS:   // the key ends inside this chain
        if (!insert_at_key_end(d, take)) return false;
        break;
    case ChainStop::Kind::DIVERGES:
    case ChainStop::Kind::PAST_END: {
        // A one-key bucket for the rest of key, as a light child of the node
        // where it leaves the path: an existing node, or a new one.
        uint64_t nb = chain_alloc(single_bucket(key, d.key_pos + at.pos + 1, take({}, {})), d.block_id);
        init_counts(nb, 1, 0);
        if (at.has_node)                               add_light_child(d, nb);
        else if (at.kind == ChainStop::Kind::DIVERGES) add_branch_node(d, nb);
        else                                           add_end_node(d, nb);
        break;
    }
    case ChainStop::Kind::TAIL:
        if (chain.tail_record.valid()) return false;  // duplicate
        chain.tail_record = take({}, {});
        break;
    case ChainStop::Kind::FOLLOW:
        break;   // descend never stops on FOLLOW
    }
    chain_write(d.block_id, std::move(chain));
    return true;
}

// Keys continuing on the chain at node ni = those past the previous node.
uint32_t DiskTrie::keys_passing(uint64_t block_id, size_t ni) {
    return ni > 0 ? get_counts(block_id).nodes[ni - 1].heavy : get_counts(block_id).total;
}

bool DiskTrie::insert_at_key_end(Descent& d, const RecordFn& take) {
    const ChainStop& at = d.stop;
    ChainData& chain = d.chain;
    if (at.has_node) {
        if (chain.nodes[at.ni].record.valid()) return false;  // duplicate
        chain.nodes[at.ni].record = take({}, {});
        return true;
    }
    // A new node holding the record, where the key ends on the path.
    uint32_t p = keys_passing(d.block_id, at.ni);
    ChainNode n;
    n.split        = static_cast<uint16_t>(at.pos);
    n.heavy_weight = floor_log2(p);
    n.record       = take({}, {});
    chain.nodes.insert(chain.nodes.begin() + static_cast<ptrdiff_t>(at.ni), n);
    auto& cn = get_counts(d.block_id).nodes;
    cn.insert(cn.begin() + static_cast<ptrdiff_t>(at.ni), NodeCounts{p, {}});
    return true;
}

void DiskTrie::add_light_child(Descent& d, uint64_t bucket) {
    size_t li = insert_light(d.chain.nodes[d.stop.ni], {d.stop.digit, 0, bucket});
    auto& lc = get_counts(d.block_id).nodes[d.stop.ni].lights;
    lc.insert(lc.begin() + static_cast<ptrdiff_t>(li), 1);
}

void DiskTrie::add_branch_node(Descent& d, uint64_t bucket) {
    // A new branch node, with the bucket as its first light child.
    uint32_t p = keys_passing(d.block_id, d.stop.ni);
    ChainNode n;
    n.split        = static_cast<uint16_t>(d.stop.pos);
    n.heavy_weight = floor_log2(p);
    n.lights.push_back({d.stop.digit, 0, bucket});
    d.chain.nodes.insert(d.chain.nodes.begin() + static_cast<ptrdiff_t>(d.stop.ni), n);
    auto& cn = get_counts(d.block_id).nodes;
    cn.insert(cn.begin() + static_cast<ptrdiff_t>(d.stop.ni), NodeCounts{p, {1}});
}

void DiskTrie::add_end_node(Descent& d, uint64_t bucket) {
    // The chain's path is a prefix of key: end the chain in a fan-out node
    // whose first child is the bucket holding the rest of key.
    ChainNode n;
    n.split = d.chain.path_len;
    n.lights.push_back({d.stop.digit, 0, bucket});
    d.chain.nodes.push_back(n);
    get_counts(d.block_id).nodes.push_back(NodeCounts{0, {1}});
}
