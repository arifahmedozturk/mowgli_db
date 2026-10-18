#include "index/disk_trie.h"
#include "index/disk_trie_internal.h"
#include <functional>
#include <shared_mutex>

// Removing a key, and pruning what it leaves empty (#38).

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
            free_slot(a);
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
