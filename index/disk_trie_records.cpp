#include "index/disk_trie.h"
#include "index/disk_trie_internal.h"
#include <algorithm>
#include <cstring>
#include <functional>
#include <shared_mutex>

// Replacing the records of keys already in the trie.

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
