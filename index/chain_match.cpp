#include "index/chain.h"
#include "index/chain_format.h"
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

// Matching a key against a chain or bucket, decoded or on its encoded bytes.

// Exact match of key's nibbles from key_pos against an encoded bucket.
static MatchOutcome bucket_match(const uint8_t* b, const std::string& key, size_t key_pos) {
    size_t tn = key.size() * 2 - key_pos;
    auto at = [&](size_t i) { return key_nibble(key, key_pos + i); };
    size_t idx = lower_bound_raw(b, tn, at);
    if (idx < bucket_count_raw(b)) {
        BucketEntryHead eh;
        const uint8_t* p = bucket_entry_raw(b, idx, eh);
        if (cmp_suffix(p, eh.nibbles, tn, at) == 0)
            return {MatchOutcome::Kind::FOUND, RecordPtr{eh.record_block, eh.record_slot}};
    }
    return {MatchOutcome::Kind::MISS};
}

// The header of an encoded bucket is sane: its size fits the buffer and the
// offsets table fits its size. Entries are checked as a search reaches them,
// so a lookup reads only the entries it probes, not the whole bucket.
static bool bucket_header_valid(const uint8_t* buf, size_t len) {
    if (len < sizeof(BucketHeader)) return false;
    BucketHeader h;
    memcpy(&h, buf, sizeof(h));
    return h.magic == BUCKET_MAGIC && h.bytes <= len && h.bytes >= sizeof(h) + 2 * size_t(h.count);
}

// bucket_match on bytes read from the file: each entry the binary search
// touches is bounds-checked (ok = false if one is corrupt).
static MatchOutcome bucket_match_checked(const uint8_t* b, size_t len, const std::string& key,
                                         size_t key_pos, bool& ok) {
    ok = false;
    if (!bucket_header_valid(b, len)) return {MatchOutcome::Kind::MISS};
    BucketHeader h;
    memcpy(&h, b, sizeof(h));
    const size_t tn = key.size() * 2 - key_pos;
    auto at    = [&](size_t i) { return key_nibble(key, key_pos + i); };
    auto entry = [&](size_t idx, BucketEntryHead& eh) -> const uint8_t* {
        uint16_t o;
        memcpy(&o, b + sizeof(BucketHeader) + 2 * idx, 2);
        if (o + sizeof(eh) > h.bytes) return nullptr;
        memcpy(&eh, b + o, sizeof(eh));
        if (o + entry_bytes(eh.nibbles) > h.bytes) return nullptr;
        return b + o + sizeof(eh);
    };
    size_t lo = 0, hi = h.count;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        BucketEntryHead eh;
        const uint8_t* p = entry(mid, eh);
        if (!p) return {MatchOutcome::Kind::MISS};
        if (cmp_suffix(p, eh.nibbles, tn, at) < 0) lo = mid + 1; else hi = mid;
    }
    ok = true;
    if (lo < h.count) {
        BucketEntryHead eh;
        const uint8_t* p = entry(lo, eh);
        if (!p) { ok = false; return {MatchOutcome::Kind::MISS}; }
        if (cmp_suffix(p, eh.nibbles, tn, at) == 0)
            return {MatchOutcome::Kind::FOUND, RecordPtr{eh.record_block, eh.record_slot}};
    }
    return {MatchOutcome::Kind::MISS};
}

// ---------- path matcher ----------

MatchOutcome chain_match(const ChainData& chain, const std::string& key, size_t key_pos) {
    if (chain.bucket) return bucket_match(chain.bucket_bytes.data(), key, key_pos);
    const size_t key_nibbles = key.size() * 2;
    size_t ni = 0;

    for (size_t i = 0; i < chain.path_len; i++) {
        bool has_node = ni < chain.nodes.size() && chain.nodes[ni].split == i;

        if (key_pos + i == key_nibbles) {
            if (has_node && chain.nodes[ni].record.valid())
                return {MatchOutcome::Kind::FOUND, chain.nodes[ni].record};
            return {MatchOutcome::Kind::MISS};
        }

        uint8_t kd = key_nibble(key, key_pos + i);
        if (kd == chain.path[i]) {
            if (has_node) ni++;
            continue;
        }
        if (!has_node) return {MatchOutcome::Kind::MISS};
        int li = chain.nodes[ni].light_index(kd);
        if (li < 0) return {MatchOutcome::Kind::MISS};
        MatchOutcome m{MatchOutcome::Kind::FOLLOW_LIGHT, {}, key_pos + i + 1, ni};
        m.light_child = chain.nodes[ni].lights[li].block;
        m.light_rank  = li;
        return m;
    }

    if (key_pos + chain.path_len == key_nibbles)
        return {chain.tail_record.valid() ? MatchOutcome::Kind::FOUND : MatchOutcome::Kind::MISS,
                chain.tail_record};
    if (ni < chain.nodes.size() && chain.nodes[ni].split == chain.path_len) {
        // End node: the next nibble picks a child; there is no heavy continuation.
        int li = chain.nodes[ni].light_index(key_nibble(key, key_pos + chain.path_len));
        if (li < 0) return {MatchOutcome::Kind::MISS};
        MatchOutcome m{MatchOutcome::Kind::FOLLOW_LIGHT, {}, key_pos + chain.path_len + 1, ni};
        m.light_child = chain.nodes[ni].lights[li].block;
        m.light_rank  = li;
        return m;
    }
    return {MatchOutcome::Kind::MISS};
}

bool chain_bytes_are_bucket(const uint8_t* buf, size_t len) {
    uint32_t magic = 0;
    if (len >= 4) memcpy(&magic, buf, 4);
    return magic == BUCKET_MAGIC;
}

MatchOutcome chain_match_raw(const uint8_t* buf, size_t len,
                             const std::string& key, size_t key_pos, bool& ok) {
    ok = false;
    if (len >= sizeof(BucketHeader)) {
        uint32_t magic;
        memcpy(&magic, buf, 4);
        if (magic == BUCKET_MAGIC) {
            return bucket_match_checked(buf, len, key, key_pos, ok);
        }
    }
    if (len < sizeof(DiskHeader)) return {MatchOutcome::Kind::MISS};
    DiskHeader hdr;
    memcpy(&hdr, buf, sizeof(hdr));
    if (hdr.magic != CHAIN_MAGIC) return {MatchOutcome::Kind::MISS};
    const uint8_t  phase = hdr.flags & 1;
    const uint8_t* path  = buf + sizeof(hdr);
    const uint8_t* node  = path + (hdr.path_len + 1) / 2;   // next node not yet passed
    const uint8_t* end   = buf + len;
    if (node > end) return {MatchOutcome::Kind::MISS};
    ok = true;

    const size_t key_nibbles = key.size() * 2;
    size_t remaining = hdr.node_count;
    DiskNodeHead head{};
    auto load_head = [&]() -> bool {
        if (remaining == 0) return true;
        if (node + sizeof(DiskNodeHead) > end) return false;
        memcpy(&head, node, sizeof(head));
        return true;
    };
    if (!load_head()) { ok = false; return {MatchOutcome::Kind::MISS}; }

    for (size_t i = 0; i < hdr.path_len; i++) {
        bool   has_node = remaining > 0 && head.split == i;
        bool   has_rec  = has_node && node_can_hold_record(phase, i);
        size_t lights   = has_node ? static_cast<size_t>(__builtin_popcount(head.light_bitmap)) : 0;
        size_t nsize    = node_bytes(has_rec, lights);
        if (has_node && node + nsize > end) { ok = false; return {MatchOutcome::Kind::MISS}; }

        if (key_pos + i == key_nibbles) {
            if (has_rec) {
                DiskRecord r;
                memcpy(&r, node + sizeof(DiskNodeHead), sizeof(r));
                RecordPtr rec{r.block, r.slot};
                if (rec.valid()) return {MatchOutcome::Kind::FOUND, rec};
            }
            return {MatchOutcome::Kind::MISS};
        }

        uint8_t kd = key_nibble(key, key_pos + i);
        uint8_t cd = (i % 2 == 0) ? (path[i / 2] >> 4) : (path[i / 2] & 0x0F);
        if (kd == cd) {
            if (has_node) {
                node += nsize;
                remaining--;
                if (!load_head()) { ok = false; return {MatchOutcome::Kind::MISS}; }
            }
            continue;
        }
        if (!has_node || !(head.light_bitmap & (1u << kd))) return {MatchOutcome::Kind::MISS};

        // Light child: index = number of lights with a smaller digit.
        size_t idx = static_cast<size_t>(__builtin_popcount(head.light_bitmap & ((1u << kd) - 1)));
        DiskLight dl;
        memcpy(&dl, node + sizeof(DiskNodeHead) + (has_rec ? sizeof(DiskRecord) : 0) + idx * sizeof(DiskLight),
               sizeof(dl));
        MatchOutcome m{MatchOutcome::Kind::FOLLOW_LIGHT, {}, key_pos + i + 1, hdr.node_count - remaining};
        m.light_child = dl.block;
        m.light_rank  = static_cast<int>(idx);
        return m;
    }

    if (key_pos + hdr.path_len == key_nibbles) {
        RecordPtr tail{hdr.tail_record_block, hdr.tail_record_slot};
        return {tail.valid() ? MatchOutcome::Kind::FOUND : MatchOutcome::Kind::MISS, tail};
    }
    if (remaining > 0 && head.split == hdr.path_len) {
        // End node: the next nibble picks a child; there is no heavy continuation.
        bool   has_rec = node_can_hold_record(phase, hdr.path_len);
        size_t lights  = static_cast<size_t>(__builtin_popcount(head.light_bitmap));
        if (node + node_bytes(has_rec, lights) > end) { ok = false; return {MatchOutcome::Kind::MISS}; }
        uint8_t kd = key_nibble(key, key_pos + hdr.path_len);
        if (!(head.light_bitmap & (1u << kd))) return {MatchOutcome::Kind::MISS};
        size_t idx = static_cast<size_t>(__builtin_popcount(head.light_bitmap & ((1u << kd) - 1)));
        DiskLight dl;
        memcpy(&dl, node + sizeof(DiskNodeHead) + (has_rec ? sizeof(DiskRecord) : 0) + idx * sizeof(DiskLight),
               sizeof(dl));
        MatchOutcome m{MatchOutcome::Kind::FOLLOW_LIGHT, {}, key_pos + hdr.path_len + 1,
                       hdr.node_count - remaining};
        m.light_child = dl.block;
        m.light_rank  = static_cast<int>(idx);
        return m;
    }
    return {MatchOutcome::Kind::MISS};
}
