#include "index/chain.h"
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

static constexpr uint32_t CHAIN_MAGIC = 0xC4A16E16;  // base-16 chain

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

static_assert(sizeof(DiskHeader)   == 19);
static_assert(sizeof(DiskNodeHead) == 6);
static_assert(sizeof(DiskRecord)   == 10);
static_assert(sizeof(DiskLight)    == 10);

static size_t node_bytes(bool has_record, size_t lights) {
    return sizeof(DiskNodeHead) + (has_record ? sizeof(DiskRecord) : 0) + lights * sizeof(DiskLight);
}

static size_t encoded_size(const ChainData& c) {
    size_t n = sizeof(DiskHeader) + (c.path_len + 1) / 2;
    for (const auto& nd : c.nodes)
        n += node_bytes(node_can_hold_record(c.phase, nd.split), nd.lights.size());
    return n;
}

static void write_bytes(const ChainData& c, uint8_t* out) {
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

static bool read_bytes(const uint8_t* buf, size_t len, ChainData& out) {
    if (len < sizeof(DiskHeader)) return false;
    DiskHeader hdr;
    memcpy(&hdr, buf, sizeof(hdr));
    if (hdr.magic != CHAIN_MAGIC) return false;

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
    return {MatchOutcome::Kind::MISS};
}

MatchOutcome chain_match_raw(const uint8_t* buf, size_t len,
                             const std::string& key, size_t key_pos, bool& ok) {
    ok = false;
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
    return {MatchOutcome::Kind::MISS};
}
