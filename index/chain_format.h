#pragma once
// The packed on-disk layout of chains and buckets and the raw-byte helpers
// shared by chain.cpp (encoding), bucket.cpp (buckets) and chain_match.cpp
// (matching); not part of the public interface (see chain.h).
#include "index/chain.h"
#include <cstddef>
#include <cstdint>
#include <cstring>

inline constexpr uint32_t CHAIN_MAGIC  = 0xC4A16E16;  // base-16 chain
inline constexpr uint32_t BUCKET_MAGIC = 0xB0C4E716;  // subtree bucket

// ---------- packed disk layout (internal) ----------
//
//   DiskHeader                          19 B
//   path, two nibbles per byte          ceil(path_len / 2) B, high nibble first
//   per node:
//     DiskNodeHead                      6 B  (split, heavy weight, light bitmap)
//     record block + slot               10 B, only where a key can end (even depth)
//     per light child, in digit order   10 B (block + weight)
//
// The 16-bit bitmap has bit d set when a light child with digit d exists, so a
// child's index is popcount(bitmap & ((1 << d) - 1)).

#pragma pack(push, 1)
struct DiskHeader {
    uint32_t magic;
    uint16_t path_len;          // nibbles
    uint16_t node_count;
    uint8_t  flags;             // bit 0 = phase
    uint64_t tail_record_block;
    uint16_t tail_record_slot;
};                              // 19 bytes

struct DiskNodeHead {
    uint16_t split;
    uint16_t heavy_weight;
    uint16_t light_bitmap;
};                              // 6 bytes

struct DiskRecord { uint64_t block; uint16_t slot;   };  // 10 bytes
struct DiskLight  { uint64_t block; uint16_t weight; };  // 10 bytes
#pragma pack(pop)

// Bucket layout:
//   BucketHeader                        10 B
//   offsets, one u16 per entry          entry start, from the bucket's first byte
//   entries, sorted by suffix:          BucketEntryHead (12 B) + packed nibbles
#pragma pack(push, 1)
struct BucketHeader {
    uint32_t magic;
    uint16_t count;
    uint16_t bytes;             // total encoded size
    uint8_t  flags;             // bit 0 = phase
    uint8_t  pad;
};                              // 10 bytes
struct BucketEntryHead {
    uint16_t nibbles;
    uint64_t record_block;
    uint16_t record_slot;
};                              // 12 bytes
#pragma pack(pop)
static_assert(sizeof(BucketHeader)    == 10);
static_assert(sizeof(BucketEntryHead) == 12);

static_assert(sizeof(DiskHeader)   == 19);
static_assert(sizeof(DiskNodeHead) == 6);
static_assert(sizeof(DiskRecord)   == 10);
static_assert(sizeof(DiskLight)    == 10);

inline size_t node_bytes(bool has_record, size_t lights) {
    return sizeof(DiskNodeHead) + (has_record ? sizeof(DiskRecord) : 0) + lights * sizeof(DiskLight);
}

inline size_t entry_bytes(size_t nibbles) { return sizeof(BucketEntryHead) + (nibbles + 1) / 2; }


inline uint16_t bucket_count_raw(const uint8_t* b) { uint16_t n; memcpy(&n, b + 4, 2); return n; }

inline const uint8_t* bucket_entry_raw(const uint8_t* b, size_t idx, BucketEntryHead& eh) {
    uint16_t o;
    memcpy(&o, b + sizeof(BucketHeader) + 2 * idx, 2);
    memcpy(&eh, b + o, sizeof(eh));
    return b + o + sizeof(eh);   // packed nibbles
}

inline uint8_t packed_nibble(const uint8_t* p, size_t i) {
    return (i % 2 == 0) ? (p[i / 2] >> 4) : (p[i / 2] & 0x0F);
}

// Compare a stored suffix (packed, n nibbles) with the target nibble sequence.
template <class TargetAt>
inline int cmp_suffix(const uint8_t* p, size_t n, size_t tn, TargetAt at) {
    size_t m = std::min(n, tn);
    for (size_t i = 0; i < m; i++) {
        uint8_t a = packed_nibble(p, i), t = at(i);
        if (a != t) return a < t ? -1 : 1;
    }
    return n < tn ? -1 : (n > tn ? 1 : 0);
}

// First entry index whose suffix is >= the target.
template <class TargetAt>
inline size_t lower_bound_raw(const uint8_t* b, size_t tn, TargetAt at) {
    size_t lo = 0, hi = bucket_count_raw(b);
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        BucketEntryHead eh;
        const uint8_t* p = bucket_entry_raw(b, mid, eh);
        if (cmp_suffix(p, eh.nibbles, tn, at) < 0) lo = mid + 1; else hi = mid;
    }
    return lo;
}


// Defined in bucket.cpp; chain decoding hands buckets to it.
bool read_bucket(const uint8_t* buf, size_t len, ChainData& out);
