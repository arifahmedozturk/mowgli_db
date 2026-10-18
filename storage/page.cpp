#include "storage/page.h"
#include <cstring>

void page_init(uint8_t* page) {
    memset(page, 0, BLOCK_SIZE);
    PageHeader phdr{};
    phdr.magic      = PAGE_MAGIC;
    phdr.slot_count = 0;
    phdr.free_end   = BLOCK_SIZE;
    set_page_header(page, phdr);
}

uint16_t page_write_row(uint8_t* page, const uint8_t* data, uint16_t size) {
    PageHeader phdr = page_header(page);

    uint16_t new_free_end = phdr.free_end - size;
    memcpy(page + new_free_end, data, size);

    // Reuse a dead slot entry (deleted or split away) before growing the directory;
    // only pages that have one are scanned.
    uint16_t slot_index = phdr.slot_count;
    for (uint16_t i = 0; phdr.dead_slots > 0 && i < phdr.slot_count; i++)
        if (page_slot(page, i).offset == 0) { slot_index = i; break; }
    set_page_slot(page, slot_index, {new_free_end, size});

    phdr.free_end = new_free_end;
    phdr.live_bytes = static_cast<uint16_t>(phdr.live_bytes + size);
    if (slot_index == phdr.slot_count) phdr.slot_count++;
    else phdr.dead_slots--;
    set_page_header(page, phdr);
    return slot_index;
}

void page_kill_row(uint8_t* page, uint16_t slot, uint16_t length) {
    set_page_slot(page, slot, {0, 0});
    PageHeader hdr = page_header(page);
    hdr.live_bytes = static_cast<uint16_t>(hdr.live_bytes - length);
    hdr.dead_slots++;
    set_page_header(page, hdr);
}

bool page_counts_match(const uint8_t* page) {
    PageHeader hdr = page_header(page);
    size_t live = 0, dead = 0;
    for (uint16_t i = 0; i < hdr.slot_count; i++) {
        PageSlot se = page_slot(page, i);
        if (se.offset != 0) live += se.length; else dead++;
    }
    return live == hdr.live_bytes && dead == hdr.dead_slots;
}
