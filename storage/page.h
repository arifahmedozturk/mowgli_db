#pragma once
// A heap page's layout and the operations on one page (#101): a header, a
// slot directory growing up from it, and rows filling down from the end of
// the block. A row's pointer is (page, slot); a deleted or moved row leaves a
// dead slot (offset 0) that a later row can take.
#include "index/chain.h"   // RecordPtr, BLOCK_SIZE
#include <cstddef>
#include <cstdint>
#include <cstring>

static constexpr uint32_t PAGE_MAGIC = 0x48504147; // "HPAG"

#pragma pack(push, 1)
struct PageHeader {
    uint32_t magic;
    uint16_t slot_count;
    uint16_t free_end;   // offset from page start where free space ends;
                         // records fill upward from BLOCK_SIZE
    uint64_t next;       // next page in key order (0 = unknown); a hint, never relied on for correctness
    uint16_t live_bytes; // bytes of live rows, so room checks need no directory scan (#74)
    uint16_t dead_slots; // directory entries of deleted or moved rows, reusable by write_row
};

struct PageSlot {
    uint16_t offset; // 0 = deleted
    uint16_t length;
};
#pragma pack(pop)

static_assert(sizeof(PageHeader) == 20);
static_assert(sizeof(PageSlot)   == 4);

inline PageHeader page_header(const uint8_t* page) {
    PageHeader h;
    memcpy(&h, page, sizeof(h));
    return h;
}
inline void set_page_header(uint8_t* page, const PageHeader& h) {
    memcpy(page, &h, sizeof(h));
}
inline PageSlot page_slot(const uint8_t* page, uint16_t slot) {
    PageSlot s;
    memcpy(&s, page + sizeof(PageHeader) + slot * sizeof(PageSlot), sizeof(PageSlot));
    return s;
}
inline void set_page_slot(uint8_t* page, uint16_t slot, PageSlot s) {
    memcpy(page + sizeof(PageHeader) + slot * sizeof(PageSlot), &s, sizeof(PageSlot));
}

// An empty page.
void     page_init(uint8_t* page);
inline uint64_t page_next(const uint8_t* page) {
    return page_header(page).next;
}
inline void page_set_next(uint8_t* page, uint64_t next) {
    PageHeader h = page_header(page);
    h.next = next;
    set_page_header(page, h);
}
// Bytes held by live rows (deleted rows' bytes stay in the page until it is
// compacted), kept in the header.
inline size_t page_live_bytes(const uint8_t* page) {
    return page_header(page).live_bytes;
}
// Free bytes after compacting: everything but the header, directory and live rows.
inline size_t page_reclaimable_space(const uint8_t* page) {
    PageHeader hdr = page_header(page);
    size_t dir_end = sizeof(PageHeader) + hdr.slot_count * sizeof(PageSlot);
    size_t used    = dir_end + hdr.live_bytes;
    return used < BLOCK_SIZE ? BLOCK_SIZE - used : 0;
}
// Free bytes between the directory and the rows, usable without compacting.
inline uint16_t page_free_space(const uint8_t* page) {
    PageHeader hdr = page_header(page);
    uint16_t dir_end = static_cast<uint16_t>(
        sizeof(PageHeader) + hdr.slot_count * sizeof(PageSlot));
    return hdr.free_end > dir_end ? hdr.free_end - dir_end : 0;
}

// Store a row in the free space (the caller checked it fits), in a dead slot
// if there is one; returns its slot.
uint16_t page_write_row(uint8_t* page, const uint8_t* data, uint16_t size);
// The live row in slot, or nullptr.
inline const uint8_t* page_row(const uint8_t* page, uint16_t slot, uint16_t& size) {
    if (slot >= page_header(page).slot_count) return nullptr;
    PageSlot se = page_slot(page, slot);
    if (se.offset == 0) return nullptr;
    size = se.length;
    return page + se.offset;
}
// Mark a live row's slot dead and update the header's counts.
void     page_kill_row(uint8_t* page, uint16_t slot, uint16_t length);
// The live-byte and dead-slot counts in the header match the directory.
bool     page_counts_match(const uint8_t* page);
