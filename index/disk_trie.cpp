#include "index/disk_trie.h"
#include <string>

// The trie is base 16: keys are matched one nibble at a time (see chain.h).
// A chain is a heavy path; each node on it branches into up to 15 light
// children, and a lookup crosses O(log n) chains because a light child holds
// at most half of its parent's keys.
//
// The implementation, by file: lookups and the shared descent (_walk),
// insert (_insert), remove (_remove), replacing records (_records), weights
// and flips (_rebalance), subtree buckets (_build), the hot cache and counts
// (_cache), commit (_commit), cursors (_cursor), compaction (_compact). This
// file holds the Bloom filter glue.

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

