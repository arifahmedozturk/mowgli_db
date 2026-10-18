#include "index/disk_trie.h"
#include "index/disk_trie_internal.h"
#include <functional>
#include <shared_mutex>
#include <stdexcept>

// Walking the trie: lookups and the descent shared by insert, remove and
// set_record (#89).

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
