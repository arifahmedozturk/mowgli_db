#include "index/chain.h"
#include "index/chain_format.h"
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

// Chain encoding and decoding (the layout is in chain_format.h).


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

