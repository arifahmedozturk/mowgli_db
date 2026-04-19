#include "index/chain.h"
#include <cstring>
#include <string>
#include <vector>
#include <zstd.h>

static constexpr uint32_t CHAIN_MAGIC = 0xC4A1B10C;

// ---------- packed disk layout (internal) ----------

#pragma pack(push, 1)
struct DiskHeader {
    uint32_t magic;
    uint16_t path_bit_len;
    uint8_t  node_count;
    uint8_t  flags;            // low 3 bits = bit_phase (0-7)
    uint64_t tail_record_block;
    uint16_t tail_record_slot;
};                             // 18 bytes

// Full node: used at positions where (bit_phase + split_bit) % 8 == 0 (byte boundary).
struct DiskNode {
    uint8_t  split_bit;
    uint16_t light_child_weight;
    uint16_t heavy_child_weight;
    uint64_t light_child_block;
    uint64_t record_block;
    uint16_t record_slot;
};                             // 23 bytes

// Lite node: used at non-byte-boundary positions — record fields omitted.
struct DiskNodeLite {
    uint8_t  split_bit;
    uint16_t light_child_weight;
    uint16_t heavy_child_weight;
    uint64_t light_child_block;
};                             // 13 bytes
#pragma pack(pop)

static_assert(sizeof(DiskHeader)   == 18);
static_assert(sizeof(DiskNode)     == 23);
static_assert(sizeof(DiskNodeLite) == 13);

// A word can end at a node only when the absolute bit depth is a multiple of 8.
// Absolute depth = bit_phase + split_bit, so this holds iff their sum % 8 == 0.
static bool node_has_record(uint8_t phase, uint8_t split_bit) {
    return (phase + split_bit) % 8 == 0;
}

// ---------------------------------------------------

bool chain_encode(const ChainData& chain, uint8_t out[BLOCK_SIZE]) {
    const size_t path_bytes = (chain.path_bit_len + 7) / 8;
    const size_t n          = chain.nodes.size();

    size_t node_bytes = 0;
    for (const auto& nd : chain.nodes)
        node_bytes += node_has_record(chain.bit_phase, nd.split_bit)
                      ? sizeof(DiskNode) : sizeof(DiskNodeLite);

    if (sizeof(DiskHeader) + path_bytes + node_bytes > BLOCK_SIZE)
        return false;

    memset(out, 0, BLOCK_SIZE);

    DiskHeader hdr{};
    hdr.magic             = CHAIN_MAGIC;
    hdr.path_bit_len      = chain.path_bit_len;
    hdr.node_count        = static_cast<uint8_t>(n);
    hdr.flags             = chain.bit_phase;
    hdr.tail_record_block = chain.tail_record.block_id;
    hdr.tail_record_slot  = chain.tail_record.slot;

    memcpy(out, &hdr, sizeof(DiskHeader));
    if (path_bytes > 0)
        memcpy(out + sizeof(DiskHeader), chain.path_bits.data(), path_bytes);

    uint8_t* p = out + sizeof(DiskHeader) + path_bytes;
    for (size_t i = 0; i < n; i++) {
        const ChainNode& nd = chain.nodes[i];
        if (node_has_record(chain.bit_phase, nd.split_bit)) {
            DiskNode dn{};
            dn.split_bit          = nd.split_bit;
            dn.light_child_weight = nd.light_child_weight;
            dn.heavy_child_weight = nd.heavy_child_weight;
            dn.light_child_block  = nd.light_child_block;
            dn.record_block       = nd.record.block_id;
            dn.record_slot        = nd.record.slot;
            memcpy(p, &dn, sizeof(DiskNode));
            p += sizeof(DiskNode);
        } else {
            DiskNodeLite dl{};
            dl.split_bit          = nd.split_bit;
            dl.light_child_weight = nd.light_child_weight;
            dl.heavy_child_weight = nd.heavy_child_weight;
            dl.light_child_block  = nd.light_child_block;
            memcpy(p, &dl, sizeof(DiskNodeLite));
            p += sizeof(DiskNodeLite);
        }
    }

    return true;
}

bool chain_decode(const uint8_t buf[BLOCK_SIZE], ChainData& out) {
    DiskHeader hdr;
    memcpy(&hdr, buf, sizeof(DiskHeader));
    if (hdr.magic != CHAIN_MAGIC) return false;

    const size_t path_bytes = (hdr.path_bit_len + 7) / 8;

    out.path_bit_len = hdr.path_bit_len;
    out.bit_phase    = hdr.flags;
    out.tail_record  = {hdr.tail_record_block, hdr.tail_record_slot};
    out.path_bits.assign(buf + sizeof(DiskHeader),
                         buf + sizeof(DiskHeader) + path_bytes);

    const uint8_t* p = buf + sizeof(DiskHeader) + path_bytes;
    out.nodes.resize(hdr.node_count);
    for (size_t i = 0; i < hdr.node_count; i++) {
        uint8_t sb;
        memcpy(&sb, p, 1);
        if (node_has_record(out.bit_phase, sb)) {
            DiskNode dn;
            memcpy(&dn, p, sizeof(DiskNode));
            p += sizeof(DiskNode);
            out.nodes[i].split_bit          = dn.split_bit;
            out.nodes[i].light_child_weight = dn.light_child_weight;
            out.nodes[i].heavy_child_weight = dn.heavy_child_weight;
            out.nodes[i].light_child_block  = dn.light_child_block;
            out.nodes[i].record             = {dn.record_block, dn.record_slot};
        } else {
            DiskNodeLite dl;
            memcpy(&dl, p, sizeof(DiskNodeLite));
            p += sizeof(DiskNodeLite);
            out.nodes[i].split_bit          = dl.split_bit;
            out.nodes[i].light_child_weight = dl.light_child_weight;
            out.nodes[i].heavy_child_weight = dl.heavy_child_weight;
            out.nodes[i].light_child_block  = dl.light_child_block;
            out.nodes[i].record             = RecordPtr{};
        }
    }

    return true;
}

// ---------- slice encode / decode (for packed block storage) ----------

size_t chain_encode_slice(const ChainData& chain, uint8_t* out, size_t max_size) {
    const size_t path_bytes = (chain.path_bit_len + 7) / 8;
    const size_t n          = chain.nodes.size();

    size_t node_bytes = 0;
    for (const auto& nd : chain.nodes)
        node_bytes += node_has_record(chain.bit_phase, nd.split_bit)
                      ? sizeof(DiskNode) : sizeof(DiskNodeLite);

    const size_t total = sizeof(DiskHeader) + path_bytes + node_bytes;
    if (total > max_size) return 0;

    DiskHeader hdr{};
    hdr.magic             = CHAIN_MAGIC;
    hdr.path_bit_len      = chain.path_bit_len;
    hdr.node_count        = static_cast<uint8_t>(n);
    hdr.flags             = chain.bit_phase;
    hdr.tail_record_block = chain.tail_record.block_id;
    hdr.tail_record_slot  = chain.tail_record.slot;
    memcpy(out, &hdr, sizeof(DiskHeader));
    if (path_bytes > 0)
        memcpy(out + sizeof(DiskHeader), chain.path_bits.data(), path_bytes);

    uint8_t* p = out + sizeof(DiskHeader) + path_bytes;
    for (size_t i = 0; i < n; i++) {
        const ChainNode& nd = chain.nodes[i];
        if (node_has_record(chain.bit_phase, nd.split_bit)) {
            DiskNode dn{};
            dn.split_bit          = nd.split_bit;
            dn.light_child_weight = nd.light_child_weight;
            dn.heavy_child_weight = nd.heavy_child_weight;
            dn.light_child_block  = nd.light_child_block;
            dn.record_block       = nd.record.block_id;
            dn.record_slot        = nd.record.slot;
            memcpy(p, &dn, sizeof(DiskNode));
            p += sizeof(DiskNode);
        } else {
            DiskNodeLite dl{};
            dl.split_bit          = nd.split_bit;
            dl.light_child_weight = nd.light_child_weight;
            dl.heavy_child_weight = nd.heavy_child_weight;
            dl.light_child_block  = nd.light_child_block;
            memcpy(p, &dl, sizeof(DiskNodeLite));
            p += sizeof(DiskNodeLite);
        }
    }
    return total;
}

bool chain_decode_slice(const uint8_t* buf, size_t len, ChainData& out) {
    if (len < sizeof(DiskHeader)) return false;
    DiskHeader hdr;
    memcpy(&hdr, buf, sizeof(DiskHeader));
    if (hdr.magic != CHAIN_MAGIC) return false;

    const size_t path_bytes = (hdr.path_bit_len + 7) / 8;
    // Conservative lower-bound check: each node is at least DiskNodeLite bytes.
    if (sizeof(DiskHeader) + path_bytes +
        (size_t)hdr.node_count * sizeof(DiskNodeLite) > len)
        return false;

    out.path_bit_len = hdr.path_bit_len;
    out.bit_phase    = hdr.flags;
    out.tail_record  = {hdr.tail_record_block, hdr.tail_record_slot};
    out.path_bits.assign(buf + sizeof(DiskHeader),
                         buf + sizeof(DiskHeader) + path_bytes);

    const uint8_t* p   = buf + sizeof(DiskHeader) + path_bytes;
    const uint8_t* end = buf + len;
    out.nodes.resize(hdr.node_count);
    for (size_t i = 0; i < hdr.node_count; i++) {
        uint8_t sb;
        memcpy(&sb, p, 1);
        if (node_has_record(out.bit_phase, sb)) {
            if (p + sizeof(DiskNode) > end) return false;
            DiskNode dn;
            memcpy(&dn, p, sizeof(DiskNode));
            p += sizeof(DiskNode);
            out.nodes[i].split_bit          = dn.split_bit;
            out.nodes[i].light_child_weight = dn.light_child_weight;
            out.nodes[i].heavy_child_weight = dn.heavy_child_weight;
            out.nodes[i].light_child_block  = dn.light_child_block;
            out.nodes[i].record             = {dn.record_block, dn.record_slot};
        } else {
            if (p + sizeof(DiskNodeLite) > end) return false;
            DiskNodeLite dl;
            memcpy(&dl, p, sizeof(DiskNodeLite));
            p += sizeof(DiskNodeLite);
            out.nodes[i].split_bit          = dl.split_bit;
            out.nodes[i].light_child_weight = dl.light_child_weight;
            out.nodes[i].heavy_child_weight = dl.heavy_child_weight;
            out.nodes[i].light_child_block  = dl.light_child_block;
            out.nodes[i].record             = RecordPtr{};
        }
    }
    return true;
}

// ---------- delta encode / decode on DiskNode uint64 fields ----------
// Applied to raw chain bytes (DiskHeader + path_bits + DiskNode[]) before
// compression. light_child_block and record_block are block addresses that
// tend to be allocated sequentially, so their deltas are small and compress well.
// NULL_BLOCK (0xFFFFFFFF'FFFFFFFF) is included in the delta sequence as-is;
// consecutive NULLs produce a delta of 0, which zstd collapses efficiently.

static void delta_encode_nodes(uint8_t* data, size_t raw_len) {
    if (raw_len < sizeof(DiskHeader)) return;
    DiskHeader hdr;
    memcpy(&hdr, data, sizeof(DiskHeader));
    if (hdr.magic != CHAIN_MAGIC || hdr.node_count == 0) return;

    const size_t path_bytes = (hdr.path_bit_len + 7) / 8;
    const size_t nodes_off  = sizeof(DiskHeader) + path_bytes;
    if (nodes_off > raw_len) return;

    const uint8_t bit_phase = hdr.flags;
    uint64_t prev_light = 0, prev_record = 0;
    uint8_t* p = data + nodes_off;
    for (uint8_t i = 0; i < hdr.node_count; i++) {
        bool full = node_has_record(bit_phase, *p);
        size_t sz = full ? sizeof(DiskNode) : sizeof(DiskNodeLite);
        if ((size_t)(p - data) + sz > raw_len) return;

        // light_child_block is at the same offset in both structs.
        uint64_t light;
        memcpy(&light, p + offsetof(DiskNodeLite, light_child_block), 8);
        uint64_t dl = light - prev_light;
        memcpy(p + offsetof(DiskNodeLite, light_child_block), &dl, 8);
        prev_light = light;

        if (full) {
            uint64_t record;
            memcpy(&record, p + offsetof(DiskNode, record_block), 8);
            uint64_t dr = record - prev_record;
            memcpy(p + offsetof(DiskNode, record_block), &dr, 8);
            prev_record = record;
        }
        p += sz;
    }
}

static void delta_decode_nodes(uint8_t* data, size_t raw_len) {
    if (raw_len < sizeof(DiskHeader)) return;
    DiskHeader hdr;
    memcpy(&hdr, data, sizeof(DiskHeader));
    if (hdr.magic != CHAIN_MAGIC || hdr.node_count == 0) return;

    const size_t path_bytes = (hdr.path_bit_len + 7) / 8;
    const size_t nodes_off  = sizeof(DiskHeader) + path_bytes;
    if (nodes_off > raw_len) return;

    const uint8_t bit_phase = hdr.flags;
    uint64_t cum_light = 0, cum_record = 0;
    uint8_t* p = data + nodes_off;
    for (uint8_t i = 0; i < hdr.node_count; i++) {
        bool full = node_has_record(bit_phase, *p);
        size_t sz = full ? sizeof(DiskNode) : sizeof(DiskNodeLite);
        if ((size_t)(p - data) + sz > raw_len) return;

        uint64_t dl;
        memcpy(&dl, p + offsetof(DiskNodeLite, light_child_block), 8);
        cum_light += dl;
        memcpy(p + offsetof(DiskNodeLite, light_child_block), &cum_light, 8);

        if (full) {
            uint64_t dr;
            memcpy(&dr, p + offsetof(DiskNode, record_block), 8);
            cum_record += dr;
            memcpy(p + offsetof(DiskNode, record_block), &cum_record, 8);
        }
        p += sz;
    }
}

// ---------- compress / decompress ----------
// Compressed envelope: [uint32 COMPRESS_MAGIC][uint32 original_size][zstd frame...]

// Compressed envelope: [uint32 COMPRESS_MAGIC][uint32 orig_size][uint32 comp_size][zstd frame]
// Storing comp_size lets the decompressor pass the exact frame length to ZSTD_decompress;
// zstd fails when srcSize is larger than the actual frame.
static constexpr size_t COMPRESS_HDR = 12;

size_t chain_compress(const uint8_t* raw, size_t raw_len,
                      uint8_t* out,       size_t out_cap) {
    // Delta-encode on a mutable copy.
    std::vector<uint8_t> tmp(raw, raw + raw_len);
    delta_encode_nodes(tmp.data(), raw_len);

    const size_t z_bound = ZSTD_compressBound(raw_len);
    if (out_cap < COMPRESS_HDR + z_bound) return 0;

    size_t z_size = ZSTD_compress(out + COMPRESS_HDR, out_cap - COMPRESS_HDR,
                                  tmp.data(), raw_len, 1);
    if (ZSTD_isError(z_size)) return 0;
    if (COMPRESS_HDR + z_size >= raw_len) return 0; // expansion — caller stores raw

    uint32_t magic = COMPRESS_MAGIC;
    uint32_t orig  = static_cast<uint32_t>(raw_len);
    uint32_t comp  = static_cast<uint32_t>(z_size);
    memcpy(out,      &magic, 4);
    memcpy(out + 4,  &orig,  4);
    memcpy(out + 8,  &comp,  4);
    return COMPRESS_HDR + z_size;
}

size_t chain_decompress(const uint8_t* in,  size_t in_len,
                        uint8_t* out,        size_t out_cap) {
    if (in_len < COMPRESS_HDR) return 0;
    uint32_t magic;
    memcpy(&magic, in, 4);
    if (magic != COMPRESS_MAGIC) return 0;

    uint32_t orig_size, comp_size;
    memcpy(&orig_size, in + 4, 4);
    memcpy(&comp_size, in + 8, 4);
    if (orig_size > out_cap || COMPRESS_HDR + comp_size > in_len) return 0;

    // Pass exact compressed-frame size so ZSTD_decompress does not overread.
    size_t n = ZSTD_decompress(out, orig_size, in + COMPRESS_HDR, comp_size);
    if (ZSTD_isError(n)) return 0;

    delta_decode_nodes(out, n);
    return n;
}

// ---------- path matcher ----------

static bool key_bit_at(const std::string& key, size_t abs) {
    return (static_cast<uint8_t>(key[abs / 8]) >> (7 - abs % 8)) & 1;
}

static bool chain_bit_at(const ChainData& chain, size_t i) {
    return (chain.path_bits[i / 8] >> (7 - i % 8)) & 1;
}

MatchOutcome chain_match(const ChainData& chain,
                         const std::string& key,
                         size_t key_bit_start) {
    const size_t key_bits = key.size() * 8;
    size_t ni = 0;

    for (size_t i = 0; i < chain.path_bit_len; i++) {
        bool has_node = ni < chain.nodes.size() && chain.nodes[ni].split_bit == i;

        if (key_bit_start + i == key_bits) {
            if (has_node && chain.nodes[ni].record.valid())
                return {MatchOutcome::Kind::FOUND, chain.nodes[ni].record};
            return {MatchOutcome::Kind::MISS};
        }

        bool kb = key_bit_at(key, key_bit_start + i);
        bool cb = chain_bit_at(chain, i);

        if (has_node) {
            if (kb != cb)
                return {MatchOutcome::Kind::FOLLOW_LIGHT, {}, key_bit_start + i + 1, ni};
            ni++;
        } else {
            if (kb != cb)
                return {MatchOutcome::Kind::MISS};
        }
    }

    if (key_bit_start + chain.path_bit_len == key_bits)
        return {chain.tail_record.valid() ? MatchOutcome::Kind::FOUND
                                          : MatchOutcome::Kind::MISS,
                chain.tail_record};

    return {MatchOutcome::Kind::MISS};
}
