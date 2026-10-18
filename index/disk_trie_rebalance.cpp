#include "index/disk_trie.h"
#include "index/disk_trie_internal.h"
#include <cassert>

// Rebalancing: subtree counts and stored weights after an insert or remove,
// and the flips they trigger.

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
    free_slot(light.block);
    return true;
}

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
