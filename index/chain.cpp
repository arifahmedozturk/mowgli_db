#include "index/chain.h"
#include <cstring>
#include <string>

static constexpr uint32_t CHAIN_MAGIC = 0xC4A1B10C;

// ---------- packed disk layout (internal) ----------

#pragma pack(push, 1)
struct DiskHeader {
    uint32_t magic;
    uint16_t path_bit_len;
    uint8_t  node_count;
    uint8_t  flags;            // reserved
    uint64_t tail_record_block;
    uint16_t tail_record_slot;
};                             // 18 bytes

struct DiskNode {
    uint8_t  split_bit;
    uint16_t light_child_weight;
    uint16_t heavy_child_weight;
    uint64_t light_child_block;
    uint64_t record_block;
    uint16_t record_slot;
};                             // 23 bytes
#pragma pack(pop)

static_assert(sizeof(DiskHeader) == 18);
static_assert(sizeof(DiskNode)   == 23);

// ---------------------------------------------------

bool chain_encode(const ChainData& chain, uint8_t out[BLOCK_SIZE]) {
    const size_t path_bytes = (chain.path_bit_len + 7) / 8;
    const size_t n          = chain.nodes.size();
    if (sizeof(DiskHeader) + path_bytes + n * sizeof(DiskNode) > BLOCK_SIZE)
        return false;

    memset(out, 0, BLOCK_SIZE);

    DiskHeader hdr{};
    hdr.magic             = CHAIN_MAGIC;
    hdr.path_bit_len      = chain.path_bit_len;
    hdr.node_count        = static_cast<uint8_t>(n);
    hdr.tail_record_block = chain.tail_record.block_id;
    hdr.tail_record_slot  = chain.tail_record.slot;

    memcpy(out, &hdr, sizeof(DiskHeader));
    memcpy(out + sizeof(DiskHeader), chain.path_bits.data(), path_bytes);

    uint8_t* p = out + sizeof(DiskHeader) + path_bytes;
    for (size_t i = 0; i < n; i++) {
        DiskNode dn{};
        dn.split_bit          = chain.nodes[i].split_bit;
        dn.light_child_weight = chain.nodes[i].light_child_weight;
        dn.heavy_child_weight = chain.nodes[i].heavy_child_weight;
        dn.light_child_block  = chain.nodes[i].light_child_block;
        dn.record_block       = chain.nodes[i].record.block_id;
        dn.record_slot        = chain.nodes[i].record.slot;
        memcpy(p, &dn, sizeof(DiskNode));
        p += sizeof(DiskNode);
    }

    return true;
}

bool chain_decode(const uint8_t buf[BLOCK_SIZE], ChainData& out) {
    DiskHeader hdr;
    memcpy(&hdr, buf, sizeof(DiskHeader));
    if (hdr.magic != CHAIN_MAGIC) return false;

    const size_t path_bytes = (hdr.path_bit_len + 7) / 8;

    out.path_bit_len       = hdr.path_bit_len;
    out.tail_record        = {hdr.tail_record_block, hdr.tail_record_slot};
    out.path_bits.assign(buf + sizeof(DiskHeader),
                         buf + sizeof(DiskHeader) + path_bytes);

    const uint8_t* p = buf + sizeof(DiskHeader) + path_bytes;
    out.nodes.resize(hdr.node_count);
    for (size_t i = 0; i < hdr.node_count; i++) {
        DiskNode dn;
        memcpy(&dn, p, sizeof(DiskNode));
        p += sizeof(DiskNode);

        out.nodes[i].split_bit          = dn.split_bit;
        out.nodes[i].light_child_weight = dn.light_child_weight;
        out.nodes[i].heavy_child_weight = dn.heavy_child_weight;
        out.nodes[i].light_child_block  = dn.light_child_block;
        out.nodes[i].record             = {dn.record_block, dn.record_slot};
    }

    return true;
}

// ---------- slice encode / decode (for packed block storage) ----------

size_t chain_encode_slice(const ChainData& chain, uint8_t* out, size_t max_size) {
    const size_t path_bytes = (chain.path_bit_len + 7) / 8;
    const size_t n          = chain.nodes.size();
    const size_t total      = sizeof(DiskHeader) + path_bytes + n * sizeof(DiskNode);
    if (total > max_size) return 0;

    DiskHeader hdr{};
    hdr.magic             = CHAIN_MAGIC;
    hdr.path_bit_len      = chain.path_bit_len;
    hdr.node_count        = static_cast<uint8_t>(n);
    hdr.tail_record_block = chain.tail_record.block_id;
    hdr.tail_record_slot  = chain.tail_record.slot;
    memcpy(out, &hdr, sizeof(DiskHeader));
    if (path_bytes > 0)
        memcpy(out + sizeof(DiskHeader), chain.path_bits.data(), path_bytes);

    uint8_t* p = out + sizeof(DiskHeader) + path_bytes;
    for (size_t i = 0; i < n; i++) {
        DiskNode dn{};
        dn.split_bit          = chain.nodes[i].split_bit;
        dn.light_child_weight = chain.nodes[i].light_child_weight;
        dn.heavy_child_weight = chain.nodes[i].heavy_child_weight;
        dn.light_child_block  = chain.nodes[i].light_child_block;
        dn.record_block       = chain.nodes[i].record.block_id;
        dn.record_slot        = chain.nodes[i].record.slot;
        memcpy(p, &dn, sizeof(DiskNode));
        p += sizeof(DiskNode);
    }
    return total;
}

bool chain_decode_slice(const uint8_t* buf, size_t len, ChainData& out) {
    if (len < sizeof(DiskHeader)) return false;
    DiskHeader hdr;
    memcpy(&hdr, buf, sizeof(DiskHeader));
    if (hdr.magic != CHAIN_MAGIC) return false;

    const size_t path_bytes = (hdr.path_bit_len + 7) / 8;
    if (sizeof(DiskHeader) + path_bytes + hdr.node_count * sizeof(DiskNode) > len)
        return false;

    out.path_bit_len = hdr.path_bit_len;
    out.tail_record  = {hdr.tail_record_block, hdr.tail_record_slot};
    out.path_bits.assign(buf + sizeof(DiskHeader),
                         buf + sizeof(DiskHeader) + path_bytes);

    const uint8_t* p = buf + sizeof(DiskHeader) + path_bytes;
    out.nodes.resize(hdr.node_count);
    for (size_t i = 0; i < hdr.node_count; i++) {
        DiskNode dn;
        memcpy(&dn, p, sizeof(DiskNode));
        p += sizeof(DiskNode);
        out.nodes[i].split_bit          = dn.split_bit;
        out.nodes[i].light_child_weight = dn.light_child_weight;
        out.nodes[i].heavy_child_weight = dn.heavy_child_weight;
        out.nodes[i].light_child_block  = dn.light_child_block;
        out.nodes[i].record             = {dn.record_block, dn.record_slot};
    }
    return true;
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
