#include "index/disk_trie.h"
#include "index/disk_trie_internal.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>
#include <shared_mutex>
#include <stdexcept>

// ---- subtree buckets: build, collect, merge ----

bool DiskTrie::fits_bucket(const std::vector<BuildItem>& items, size_t lo, size_t hi, size_t depth) const {
    size_t n = bucket_base_bytes();
    for (size_t i = lo; i < hi; i++) {
        n += bucket_entry_bytes(items[i].nib.size() - depth);
        if (n > opts_.bucket_max_bytes) return false;
    }
    return true;
}

ChainData DiskTrie::build_node(const std::vector<BuildItem>& items, size_t lo, size_t hi,
                               size_t depth, size_t abs_start, uint64_t hint, ChainCounts& cc) {
    cc = ChainCounts{};
    cc.total = static_cast<uint32_t>(hi - lo);
    if (fits_bucket(items, lo, hi, depth)) {
        std::vector<BucketEntry> entries;
        entries.reserve(hi - lo);
        for (size_t i = lo; i < hi; i++)
            entries.push_back({std::vector<uint8_t>(items[i].nib.begin() + static_cast<ptrdiff_t>(depth),
                                                    items[i].nib.end()),
                               items[i].rec});
        return make_bucket(entries, static_cast<uint8_t>(abs_start % 2));
    }

    // A chain along the heavy path; light subtrees are built recursively.
    ChainData c;
    c.phase = static_cast<uint8_t>(abs_start % 2);
    size_t pos = depth;   // nibble index into every item
    while (true) {
        RecordPtr here{};
        if (items[lo].nib.size() == pos) { here = items[lo].rec; lo++; }   // sorted: shortest first
        if (lo == hi) { c.tail_record = here; break; }

        // Group the remaining items by their nibble at pos (contiguous, since sorted).
        std::vector<std::pair<size_t, size_t>> groups;
        for (size_t a = lo; a < hi;) {
            size_t  b = a;
            uint8_t d = items[a].nib[pos];
            while (b < hi && items[b].nib[pos] == d) b++;
            groups.push_back({a, b});
            a = b;
        }
        if (groups.size() == 1 && !here.valid()) {   // no branch here: just a path nibble
            c.path.push_back(items[lo].nib[pos]);
            pos++;
            continue;
        }

        size_t heavy = 0;
        for (size_t g = 1; g < groups.size(); g++)
            if (groups[g].second - groups[g].first > groups[heavy].second - groups[heavy].first) heavy = g;

        size_t child_abs = abs_start + (pos - depth) + 1;
        auto light_for = [&](size_t g) -> LightChild {
            auto [a, b] = groups[g];
            uint64_t addr = build_alloc(items, a, b, pos + 1, child_abs, hint);
            return {items[a].nib[pos], floor_log2(static_cast<uint32_t>(b - a)), addr};
        };

        ChainNode  n;
        NodeCounts nc;
        n.split = static_cast<uint16_t>(c.path.size());
        if (fits_bucket(items, groups[heavy].first, groups[heavy].second, pos + 1)) {
            // End node: even the heavy side fits a bucket, so the chain stops here
            // and every child, the heaviest included, hangs off as a light child.
            for (size_t g = 0; g < groups.size(); g++) {
                n.lights.push_back(light_for(g));
                nc.lights.push_back(static_cast<uint32_t>(groups[g].second - groups[g].first));
            }
            c.tail_record = here;
            c.nodes.push_back(std::move(n));
            cc.nodes.push_back(std::move(nc));
            break;
        }
        n.record = here;
        for (size_t g = 0; g < groups.size(); g++) {
            if (g == heavy) continue;
            n.lights.push_back(light_for(g));
            nc.lights.push_back(static_cast<uint32_t>(groups[g].second - groups[g].first));
        }
        nc.heavy       = static_cast<uint32_t>(groups[heavy].second - groups[heavy].first);
        n.heavy_weight = floor_log2(nc.heavy);
        c.nodes.push_back(std::move(n));
        cc.nodes.push_back(std::move(nc));
        c.path.push_back(items[groups[heavy].first].nib[pos]);
        lo = groups[heavy].first;
        hi = groups[heavy].second;
        pos++;
    }
    if (c.path.size() > UINT16_MAX) throw std::runtime_error("key too long for a chain");
    c.path_len = static_cast<uint16_t>(c.path.size());
    return c;
}

uint64_t DiskTrie::build_alloc(const std::vector<BuildItem>& items, size_t lo, size_t hi,
                               size_t depth, size_t abs_start, uint64_t hint) {
    ChainCounts cc;
    ChainData   d    = build_node(items, lo, hi, depth, abs_start, hint, cc);
    uint64_t    addr = chain_alloc(d, hint);
    counts_[addr] = std::move(cc);
    return addr;
}

void DiskTrie::collect_subtree(uint64_t addr, const std::vector<uint8_t>& prefix,
                               std::vector<BucketEntry>& out, std::vector<uint64_t>& addrs) {
    addrs.push_back(addr);
    ChainData c = chain_read(addr);
    if (c.bucket) {
        std::vector<BucketEntry> es;
        bucket_decode(c, es);
        for (auto& e : es) {
            std::vector<uint8_t> s = prefix;
            s.insert(s.end(), e.suffix.begin(), e.suffix.end());
            out.push_back({std::move(s), e.record});
        }
        return;
    }
    auto upto = [&](size_t n) {
        std::vector<uint8_t> s = prefix;
        s.insert(s.end(), c.path.begin(), c.path.begin() + static_cast<ptrdiff_t>(n));
        return s;
    };
    for (const auto& node : c.nodes) {
        if (node.record.valid() && node.split < c.path_len) out.push_back({upto(node.split), node.record});
        for (const auto& l : node.lights) {
            std::vector<uint8_t> p = upto(node.split);
            p.push_back(l.digit);
            collect_subtree(l.block, p, out, addrs);
        }
    }
    if (c.tail_record.valid()) out.push_back({upto(c.path_len), c.tail_record});
}

bool DiskTrie::merge_to_bucket(uint64_t addr, size_t abs_start, uint64_t parent, size_t ni, int li) {
    if (chain_read(addr).bucket) return false;
    std::vector<BucketEntry> entries;
    std::vector<uint64_t>    addrs;
    collect_subtree(addr, {}, entries, addrs);
    // Leave room to grow, so a merged bucket doesn't burst again straight away.
    if (bucket_encoded_size(entries) > opts_.bucket_max_bytes / 2) return false;
    std::sort(entries.begin(), entries.end(), suffix_less);

    for (size_t i = 1; i < addrs.size(); i++) {   // addrs[0] is the subtree root, moved below
        hot_discard(addrs[i]);
        counts_.erase(addrs[i]);
        dm_.free_chain_slot(addrs[i]);
    }
    // The bucket gets a slot of its own size class; the parent points at it.
    uint64_t moved = move_bucket(addr, make_bucket(entries, static_cast<uint8_t>(abs_start % 2)), parent, ni, li);
    counts_[moved] = ChainCounts{static_cast<uint32_t>(entries.size()), {}};
    bucket_merges_++;
    return true;
}

void DiskTrie::bulk_insert(std::vector<std::pair<std::string, RecordPtr>> kvs) {
    std::unique_lock<std::shared_mutex> lock(trie_latch_);
    if (dm_.root_block() != NULL_BLOCK) {
        // Into a non-empty trie: regular inserts keep the structure balanced.
        ensure_counts();
        for (auto& [key, ptr] : kvs) insert_locked(key, [p = ptr](RecordPtr, RecordPtr) { return p; });
        return;
    }

    // Empty trie: build the whole structure bottom-up from the sorted keys.
    std::stable_sort(kvs.begin(), kvs.end(),
                     [](const auto& a, const auto& b) { return a.first < b.first; });
    kvs.erase(std::unique(kvs.begin(), kvs.end(),
                          [](const auto& a, const auto& b) { return a.first == b.first; }),
              kvs.end());
    if (kvs.empty()) return;

    if (bloom_.empty()) bloom_.reserve(kvs.size());
    std::vector<BuildItem> items(kvs.size());
    for (size_t i = 0; i < kvs.size(); i++) {
        bloom_add(kvs[i].first);
        items[i] = {key_suffix(kvs[i].first, 0), kvs[i].second};
    }
    uint64_t root = build_alloc(items, 0, items.size(), 0, 0, NULL_BLOCK);
    dm_.set_root_block(root);
    counts_ready_ = true;
}

