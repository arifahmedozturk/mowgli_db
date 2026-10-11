#include "index/chain.h"
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

static constexpr uint32_t CHAIN_MAGIC  = 0xC4A16E16;  // base-16 chain
static constexpr uint32_t BUCKET_MAGIC = 0xB0C4E716;  // subtree bucket

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

static size_t node_bytes(bool has_record, size_t lights) {
    return sizeof(DiskNodeHead) + (has_record ? sizeof(DiskRecord) : 0) + lights * sizeof(DiskLight);
}

static size_t encoded_size(const ChainData& c) {
    if (c.bucket) return c.bucket_bytes.size();
    size_t n = sizeof(DiskHeader) + (c.path_len + 1) / 2;
    for (const auto& nd : c.nodes)
        n += node_bytes(node_can_hold_record(c.phase, nd.split), nd.lights.size());
    return n;
}

static void write_bytes(const ChainData& c, uint8_t* out) {
    if (c.bucket) { memcpy(out, c.bucket_bytes.data(), c.bucket_bytes.size()); return; }
    DiskHeader hdr{};
    hdr.magic             = CHAIN_MAGIC;
    hdr.path_len          = c.path_len;
    hdr.node_count        = static_cast<uint16_t>(c.nodes.size());
    hdr.flags             = c.phase & 1;
    hdr.tail_record_block = c.tail_record.block_id;
    hdr.tail_record_slot  = c.tail_record.slot;
    memcpy(out, &hdr, sizeof(hdr));

    uint8_t* p = out + sizeof(hdr);
    for (size_t i = 0; i < c.path_len; i += 2) {
        uint8_t hi = c.path[i] & 0x0F;
        uint8_t lo = (i + 1 < c.path_len) ? (c.path[i + 1] & 0x0F) : 0;
        *p++ = static_cast<uint8_t>(hi << 4 | lo);
    }

    for (const auto& nd : c.nodes) {
        DiskNodeHead h{nd.split, nd.heavy_weight, 0};
        for (const auto& l : nd.lights) h.light_bitmap |= static_cast<uint16_t>(1u << l.digit);
        memcpy(p, &h, sizeof(h));
        p += sizeof(h);
        if (node_can_hold_record(c.phase, nd.split)) {
            DiskRecord r{nd.record.block_id, nd.record.slot};
            memcpy(p, &r, sizeof(r));
            p += sizeof(r);
        }
        for (const auto& l : nd.lights) {   // lights are kept sorted by digit
            DiskLight dl{l.block, l.weight};
            memcpy(p, &dl, sizeof(dl));
            p += sizeof(dl);
        }
    }
}

static bool read_bucket(const uint8_t* buf, size_t len, ChainData& out);

static bool read_bytes(const uint8_t* buf, size_t len, ChainData& out) {
    if (len >= sizeof(BucketHeader)) {
        uint32_t magic;
        memcpy(&magic, buf, 4);
        if (magic == BUCKET_MAGIC) return read_bucket(buf, len, out);
    }
    if (len < sizeof(DiskHeader)) return false;
    DiskHeader hdr;
    memcpy(&hdr, buf, sizeof(hdr));
    if (hdr.magic != CHAIN_MAGIC) return false;
    out.bucket = false;
    out.bucket_bytes.clear();

    const uint8_t* p   = buf + sizeof(hdr);
    const uint8_t* end = buf + len;
    const size_t path_bytes = (hdr.path_len + 1) / 2;
    if (p + path_bytes > end) return false;

    out.path_len    = hdr.path_len;
    out.phase       = hdr.flags & 1;
    out.tail_record = {hdr.tail_record_block, hdr.tail_record_slot};
    out.path.resize(hdr.path_len);
    for (size_t i = 0; i < hdr.path_len; i++)
        out.path[i] = (i % 2 == 0) ? (p[i / 2] >> 4) : (p[i / 2] & 0x0F);
    p += path_bytes;

    out.nodes.resize(hdr.node_count);
    for (auto& nd : out.nodes) {
        if (p + sizeof(DiskNodeHead) > end) return false;
        DiskNodeHead h;
        memcpy(&h, p, sizeof(h));
        p += sizeof(h);
        bool has_rec = node_can_hold_record(out.phase, h.split);
        if (p + node_bytes(has_rec, __builtin_popcount(h.light_bitmap)) - sizeof(h) > end) return false;
        nd.split        = h.split;
        nd.heavy_weight = h.heavy_weight;
        nd.record       = RecordPtr{};
        if (has_rec) {
            DiskRecord r;
            memcpy(&r, p, sizeof(r));
            p += sizeof(r);
            nd.record = {r.block, r.slot};
        }
        nd.lights.clear();
        for (uint8_t d = 0; d < NIBBLE_FANOUT; d++) {
            if (!(h.light_bitmap & (1u << d))) continue;
            DiskLight dl;
            memcpy(&dl, p, sizeof(dl));
            p += sizeof(dl);
            nd.lights.push_back({d, dl.weight, dl.block});
        }
    }
    return true;
}

// ---------- buckets ----------

static size_t entry_bytes(size_t nibbles) { return sizeof(BucketEntryHead) + (nibbles + 1) / 2; }

size_t bucket_base_bytes() { return sizeof(BucketHeader); }
size_t bucket_entry_bytes(size_t suffix_nibbles) { return 2 + entry_bytes(suffix_nibbles); }

size_t bucket_encoded_size(const std::vector<BucketEntry>& entries) {
    size_t n = sizeof(BucketHeader);
    for (const auto& e : entries) n += 2 + entry_bytes(e.suffix.size());
    return n;
}

ChainData make_bucket(const std::vector<BucketEntry>& entries, uint8_t phase) {
    ChainData c;
    c.bucket = true;
    c.phase  = phase & 1;
    size_t total = bucket_encoded_size(entries);
    c.bucket_bytes.assign(total, 0);
    if (total > UINT16_MAX) return c;  // caller checks the size against BLOCK_SIZE first
    uint8_t* b = c.bucket_bytes.data();
    BucketHeader h{BUCKET_MAGIC, static_cast<uint16_t>(entries.size()), static_cast<uint16_t>(total),
                   static_cast<uint8_t>(c.phase), 0};
    memcpy(b, &h, sizeof(h));
    size_t off = sizeof(h) + 2 * entries.size();
    for (size_t i = 0; i < entries.size(); i++) {
        const auto& e = entries[i];
        uint16_t o = static_cast<uint16_t>(off);
        memcpy(b + sizeof(h) + 2 * i, &o, 2);
        BucketEntryHead eh{static_cast<uint16_t>(e.suffix.size()), e.record.block_id, e.record.slot};
        memcpy(b + off, &eh, sizeof(eh));
        uint8_t* p = b + off + sizeof(eh);
        for (size_t k = 0; k < e.suffix.size(); k += 2) {
            uint8_t hi = e.suffix[k] & 0x0F;
            uint8_t lo = (k + 1 < e.suffix.size()) ? (e.suffix[k + 1] & 0x0F) : 0;
            *p++ = static_cast<uint8_t>(hi << 4 | lo);
        }
        off += entry_bytes(e.suffix.size());
    }
    return c;
}

// Validate an encoded bucket: header, offsets and entries inside its bytes.
static bool bucket_valid(const uint8_t* buf, size_t len) {
    if (len < sizeof(BucketHeader)) return false;
    BucketHeader h;
    memcpy(&h, buf, sizeof(h));
    if (h.magic != BUCKET_MAGIC || h.bytes > len || h.bytes < sizeof(h) + 2 * size_t(h.count)) return false;
    for (size_t i = 0; i < h.count; i++) {
        uint16_t o;
        memcpy(&o, buf + sizeof(h) + 2 * i, 2);
        if (o + sizeof(BucketEntryHead) > h.bytes) return false;
        BucketEntryHead eh;
        memcpy(&eh, buf + o, sizeof(eh));
        if (o + entry_bytes(eh.nibbles) > h.bytes) return false;
    }
    return true;
}

static bool read_bucket(const uint8_t* buf, size_t len, ChainData& out) {
    if (!bucket_valid(buf, len)) return false;
    BucketHeader h;
    memcpy(&h, buf, sizeof(h));
    out = ChainData{};
    out.bucket = true;
    out.phase  = h.flags & 1;
    out.bucket_bytes.assign(buf, buf + h.bytes);
    return true;
}

static uint16_t bucket_count_raw(const uint8_t* b) { uint16_t n; memcpy(&n, b + 4, 2); return n; }

static const uint8_t* bucket_entry_raw(const uint8_t* b, size_t idx, BucketEntryHead& eh) {
    uint16_t o;
    memcpy(&o, b + sizeof(BucketHeader) + 2 * idx, 2);
    memcpy(&eh, b + o, sizeof(eh));
    return b + o + sizeof(eh);   // packed nibbles
}

static uint8_t packed_nibble(const uint8_t* p, size_t i) {
    return (i % 2 == 0) ? (p[i / 2] >> 4) : (p[i / 2] & 0x0F);
}

// Compare a stored suffix (packed, n nibbles) with the target nibble sequence.
template <class TargetAt>
static int cmp_suffix(const uint8_t* p, size_t n, size_t tn, TargetAt at) {
    size_t m = std::min(n, tn);
    for (size_t i = 0; i < m; i++) {
        uint8_t a = packed_nibble(p, i), t = at(i);
        if (a != t) return a < t ? -1 : 1;
    }
    return n < tn ? -1 : (n > tn ? 1 : 0);
}

// First entry index whose suffix is >= the target.
template <class TargetAt>
static size_t lower_bound_raw(const uint8_t* b, size_t tn, TargetAt at) {
    size_t lo = 0, hi = bucket_count_raw(b);
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        BucketEntryHead eh;
        const uint8_t* p = bucket_entry_raw(b, mid, eh);
        if (cmp_suffix(p, eh.nibbles, tn, at) < 0) lo = mid + 1; else hi = mid;
    }
    return lo;
}

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

bool bucket_decode(const ChainData& bucket, std::vector<BucketEntry>& out) {
    if (!bucket.bucket) return false;
    const uint8_t* b = bucket.bucket_bytes.data();
    size_t n = bucket_count_raw(b);
    out.resize(n);
    for (size_t i = 0; i < n; i++) bucket_entry(bucket, i, out[i].suffix, out[i].record);
    return true;
}

size_t bucket_size(const ChainData& bucket) { return bucket_count_raw(bucket.bucket_bytes.data()); }

void bucket_entry(const ChainData& bucket, size_t idx, std::vector<uint8_t>& suffix, RecordPtr& rec) {
    BucketEntryHead eh;
    const uint8_t* p = bucket_entry_raw(bucket.bucket_bytes.data(), idx, eh);
    suffix.resize(eh.nibbles);
    for (size_t i = 0; i < eh.nibbles; i++) suffix[i] = packed_nibble(p, i);
    rec = {eh.record_block, eh.record_slot};
}

size_t bucket_lower_bound(const ChainData& bucket, const std::vector<uint8_t>& target) {
    return lower_bound_raw(bucket.bucket_bytes.data(), target.size(),
                           [&](size_t i) { return target[i]; });
}

bool bucket_entry_equals(const ChainData& bucket, size_t idx, const std::vector<uint8_t>& suffix) {
    const uint8_t* b = bucket.bucket_bytes.data();
    if (idx >= bucket_count_raw(b)) return false;
    BucketEntryHead eh;
    const uint8_t* p = bucket_entry_raw(b, idx, eh);
    return cmp_suffix(p, eh.nibbles, suffix.size(), [&](size_t i) { return suffix[i]; }) == 0;
}

// Entries are stored in index order, so an edit is: offsets table +/- one slot,
// entries before idx shift by the offsets change, entries after idx shift by
// that plus the edited entry's size.
static void bucket_set_header(std::vector<uint8_t>& v, uint16_t count) {
    BucketHeader h;
    memcpy(&h, v.data(), sizeof(h));
    h.count = count;
    h.bytes = static_cast<uint16_t>(v.size());
    memcpy(v.data(), &h, sizeof(h));
}

void bucket_insert_at(ChainData& bucket, size_t idx, const std::vector<uint8_t>& suffix, RecordPtr rec) {
    const std::vector<uint8_t>& old = bucket.bucket_bytes;
    const size_t n       = bucket_count_raw(old.data());
    const size_t old_dir = sizeof(BucketHeader) + 2 * n;
    // Where entry idx starts in the old encoding (end of entries if appending).
    size_t split = old.size();
    if (idx < n) { uint16_t o; memcpy(&o, old.data() + sizeof(BucketHeader) + 2 * idx, 2); split = o; }
    const size_t eb = entry_bytes(suffix.size());

    std::vector<uint8_t> v(old.size() + 2 + eb);
    memcpy(v.data(), old.data(), sizeof(BucketHeader));
    for (size_t i = 0, j = 0; i <= n; i++) {          // new offsets table
        uint16_t o;
        if (i == idx) o = static_cast<uint16_t>(split + 2);
        else {
            memcpy(&o, old.data() + sizeof(BucketHeader) + 2 * j, 2);
            o = static_cast<uint16_t>(o + 2 + (j >= idx ? eb : 0));
            j++;
        }
        memcpy(v.data() + sizeof(BucketHeader) + 2 * i, &o, 2);
    }
    uint8_t* w = v.data() + old_dir + 2;
    memcpy(w, old.data() + old_dir, split - old_dir);   // entries before idx
    w += split - old_dir;
    BucketEntryHead eh{static_cast<uint16_t>(suffix.size()), rec.block_id, rec.slot};
    memcpy(w, &eh, sizeof(eh));
    uint8_t* p = w + sizeof(eh);
    for (size_t k = 0; k < suffix.size(); k += 2) {
        uint8_t hi = suffix[k] & 0x0F;
        uint8_t lo = (k + 1 < suffix.size()) ? (suffix[k + 1] & 0x0F) : 0;
        *p++ = static_cast<uint8_t>(hi << 4 | lo);
    }
    w += eb;
    memcpy(w, old.data() + split, old.size() - split);   // entries from idx on
    bucket_set_header(v, static_cast<uint16_t>(n + 1));
    bucket.bucket_bytes = std::move(v);
}

void bucket_erase_at(ChainData& bucket, size_t idx) {
    const std::vector<uint8_t>& old = bucket.bucket_bytes;
    const size_t n       = bucket_count_raw(old.data());
    const size_t old_dir = sizeof(BucketHeader) + 2 * n;
    BucketEntryHead eh;
    uint16_t start;
    memcpy(&start, old.data() + sizeof(BucketHeader) + 2 * idx, 2);
    memcpy(&eh, old.data() + start, sizeof(eh));
    const size_t eb = entry_bytes(eh.nibbles);

    std::vector<uint8_t> v(old.size() - 2 - eb);
    memcpy(v.data(), old.data(), sizeof(BucketHeader));
    for (size_t i = 0, j = 0; j < n; j++) {
        if (j == idx) continue;
        uint16_t o;
        memcpy(&o, old.data() + sizeof(BucketHeader) + 2 * j, 2);
        o = static_cast<uint16_t>(o - 2 - (j > idx ? eb : 0));
        memcpy(v.data() + sizeof(BucketHeader) + 2 * i, &o, 2);
        i++;
    }
    uint8_t* w = v.data() + old_dir - 2;
    memcpy(w, old.data() + old_dir, start - old_dir);
    w += start - old_dir;
    memcpy(w, old.data() + start + eb, old.size() - start - eb);
    bucket_set_header(v, static_cast<uint16_t>(n - 1));
    bucket.bucket_bytes = std::move(v);
}

size_t chain_encoded_size(const ChainData& chain) { return encoded_size(chain); }

// ---------------------------------------------------

bool chain_encode(const ChainData& chain, uint8_t out[BLOCK_SIZE]) {
    if (encoded_size(chain) > BLOCK_SIZE) return false;
    memset(out, 0, BLOCK_SIZE);
    write_bytes(chain, out);
    return true;
}

bool chain_decode(const uint8_t buf[BLOCK_SIZE], ChainData& out) {
    return read_bytes(buf, BLOCK_SIZE, out);
}

// ---------- slice encode / decode (for packed block storage) ----------

size_t chain_encode_slice(const ChainData& chain, uint8_t* out, size_t max_size) {
    size_t total = encoded_size(chain);
    if (total > max_size) return 0;
    write_bytes(chain, out);
    return total;
}

bool chain_decode_slice(const uint8_t* buf, size_t len, ChainData& out) {
    return read_bytes(buf, len, out);
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
        return m;
    }
    return {MatchOutcome::Kind::MISS};
}

MatchOutcome chain_match_raw(const uint8_t* buf, size_t len,
                             const std::string& key, size_t key_pos, bool& ok) {
    ok = false;
    if (len >= sizeof(BucketHeader)) {
        uint32_t magic;
        memcpy(&magic, buf, 4);
        if (magic == BUCKET_MAGIC) {
            if (!bucket_valid(buf, len)) return {MatchOutcome::Kind::MISS};
            ok = true;
            return bucket_match(buf, key, key_pos);
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
        return m;
    }
    return {MatchOutcome::Kind::MISS};
}
