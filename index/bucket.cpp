#include "index/chain.h"
#include "index/chain_format.h"
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

// Subtree buckets: building, decoding, and edits on a ChainData or in place
// on the encoded bytes.

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

bool read_bucket(const uint8_t* buf, size_t len, ChainData& out) {
    if (!bucket_valid(buf, len)) return false;
    BucketHeader h;
    memcpy(&h, buf, sizeof(h));
    out = ChainData{};
    out.bucket = true;
    out.phase  = h.flags & 1;
    out.bucket_bytes.assign(buf, buf + h.bytes);
    return true;
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

void bucket_entry_append_key(const ChainData& bucket, size_t idx, int odd_nibble,
                             std::string& key, RecordPtr& rec) {
    BucketEntryHead eh;
    const uint8_t* p = bucket_entry_raw(bucket.bucket_bytes.data(), idx, eh);
    rec = {eh.record_block, eh.record_slot};
    const size_t n = eh.nibbles;
    if (odd_nibble < 0) {   // byte-aligned: the packed nibbles are the key bytes
        key.append(reinterpret_cast<const char*>(p), n / 2);
        return;
    }
    // The pending nibble and the suffix's first one make a byte, and so on.
    key.push_back(static_cast<char>(odd_nibble << 4 | packed_nibble(p, 0)));
    for (size_t i = 1; i + 1 < n; i += 2)
        key.push_back(static_cast<char>(packed_nibble(p, i) << 4 | packed_nibble(p, i + 1)));
}

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

RecordPtr bucket_record(const ChainData& bucket, size_t idx) {
    BucketEntryHead eh;
    bucket_entry_raw(bucket.bucket_bytes.data(), idx, eh);
    return {eh.record_block, eh.record_slot};
}

void bucket_set_record(ChainData& bucket, size_t idx, RecordPtr rec) {
    uint8_t* b = bucket.bucket_bytes.data();
    uint16_t o;
    memcpy(&o, b + sizeof(BucketHeader) + 2 * idx, 2);
    memcpy(b + o + offsetof(BucketEntryHead, record_block), &rec.block_id, 8);
    memcpy(b + o + offsetof(BucketEntryHead, record_slot),  &rec.slot, 2);
}

bool bucket_entry_equals(const ChainData& bucket, size_t idx, const std::vector<uint8_t>& suffix) {
    const uint8_t* b = bucket.bucket_bytes.data();
    if (idx >= bucket_count_raw(b)) return false;
    BucketEntryHead eh;
    const uint8_t* p = bucket_entry_raw(b, idx, eh);
    return cmp_suffix(p, eh.nibbles, suffix.size(), [&](size_t i) { return suffix[i]; }) == 0;
}

// ---- bucket edits on the encoded bytes, wherever they live (#73) ----

size_t bucket_count_raw_bytes(const uint8_t* b) { return bucket_count_raw(b); }

size_t bucket_size_raw(const uint8_t* b) {
    BucketHeader h;
    memcpy(&h, b, sizeof(h));
    return h.bytes;
}

size_t bucket_lower_bound_raw(const uint8_t* b, const std::vector<uint8_t>& target) {
    return lower_bound_raw(b, target.size(), [&](size_t i) { return target[i]; });
}

bool bucket_entry_equals_raw(const uint8_t* b, size_t idx, const std::vector<uint8_t>& suffix) {
    if (idx >= bucket_count_raw(b)) return false;
    BucketEntryHead eh;
    const uint8_t* p = bucket_entry_raw(b, idx, eh);
    return cmp_suffix(p, eh.nibbles, suffix.size(), [&](size_t i) { return suffix[i]; }) == 0;
}

RecordPtr bucket_record_raw(const uint8_t* b, size_t idx) {
    BucketEntryHead eh;
    bucket_entry_raw(b, idx, eh);
    return {eh.record_block, eh.record_slot};
}

void bucket_set_record_raw(uint8_t* b, size_t idx, RecordPtr rec) {
    uint16_t o;
    memcpy(&o, b + sizeof(BucketHeader) + 2 * idx, 2);
    memcpy(b + o + offsetof(BucketEntryHead, record_block), &rec.block_id, 8);
    memcpy(b + o + offsetof(BucketEntryHead, record_slot),  &rec.slot, 2);
}

// Entries are stored in index order, so an edit is: offsets table +/- one slot,
// entries before idx shift by the offsets change, entries after idx shift by
// that plus the edited entry's size. Everything moves within the buffer.
void bucket_insert_raw(uint8_t* b, size_t idx, const std::vector<uint8_t>& suffix, RecordPtr rec) {
    BucketHeader h;
    memcpy(&h, b, sizeof(h));
    const size_t n = h.count, total = h.bytes, old_dir = sizeof(h) + 2 * n;
    uint8_t* offs = b + sizeof(h);
    size_t split = total;   // where entry idx starts (end of entries if appending)
    if (idx < n) { uint16_t o; memcpy(&o, offs + 2 * idx, 2); split = o; }
    const size_t eb = entry_bytes(suffix.size());

    memmove(b + split + 2 + eb, b + split, total - split);       // entries from idx on
    memmove(b + old_dir + 2, b + old_dir, split - old_dir);      // entries before idx
    memmove(offs + 2 * (idx + 1), offs + 2 * idx, 2 * (n - idx)); // open offset slot idx
    for (size_t i = 0; i <= n; i++) {
        uint16_t o;
        if (i == idx) o = static_cast<uint16_t>(split + 2);
        else {
            memcpy(&o, offs + 2 * i, 2);
            o = static_cast<uint16_t>(o + 2 + (i > idx ? eb : 0));
        }
        memcpy(offs + 2 * i, &o, 2);
    }
    uint8_t* w = b + split + 2;
    BucketEntryHead eh{static_cast<uint16_t>(suffix.size()), rec.block_id, rec.slot};
    memcpy(w, &eh, sizeof(eh));
    uint8_t* p = w + sizeof(eh);
    for (size_t k = 0; k < suffix.size(); k += 2) {
        uint8_t hi = suffix[k] & 0x0F;
        uint8_t lo = (k + 1 < suffix.size()) ? (suffix[k + 1] & 0x0F) : 0;
        *p++ = static_cast<uint8_t>(hi << 4 | lo);
    }
    h.count = static_cast<uint16_t>(n + 1);
    h.bytes = static_cast<uint16_t>(total + 2 + eb);
    memcpy(b, &h, sizeof(h));
}

void bucket_erase_raw(uint8_t* b, size_t idx) {
    BucketHeader h;
    memcpy(&h, b, sizeof(h));
    const size_t n = h.count, total = h.bytes, old_dir = sizeof(h) + 2 * n;
    uint8_t* offs = b + sizeof(h);
    uint16_t start;
    memcpy(&start, offs + 2 * idx, 2);
    BucketEntryHead eh;
    memcpy(&eh, b + start, sizeof(eh));
    const size_t eb = entry_bytes(eh.nibbles);

    memmove(offs + 2 * idx, offs + 2 * (idx + 1), 2 * (n - idx - 1));  // close offset slot idx
    for (size_t i = 0; i + 1 < n; i++) {
        uint16_t o;
        memcpy(&o, offs + 2 * i, 2);
        o = static_cast<uint16_t>(o - 2 - (i >= idx ? eb : 0));
        memcpy(offs + 2 * i, &o, 2);
    }
    memmove(b + old_dir - 2, b + old_dir, start - old_dir);               // entries before idx
    memmove(b + start - 2, b + start + eb, total - start - eb);           // entries after idx
    memset(b + total - 2 - eb, 0, 2 + eb);
    h.count = static_cast<uint16_t>(n - 1);
    h.bytes = static_cast<uint16_t>(total - 2 - eb);
    memcpy(b, &h, sizeof(h));
}

void bucket_insert_at(ChainData& bucket, size_t idx, const std::vector<uint8_t>& suffix, RecordPtr rec) {
    bucket.bucket_bytes.resize(bucket.bucket_bytes.size() + 2 + entry_bytes(suffix.size()));
    bucket_insert_raw(bucket.bucket_bytes.data(), idx, suffix, rec);
}

void bucket_erase_at(ChainData& bucket, size_t idx) {
    bucket_erase_raw(bucket.bucket_bytes.data(), idx);
    bucket.bucket_bytes.resize(bucket_size_raw(bucket.bucket_bytes.data()));
}

