#include "storage/heap.h"
#include "storage/page.h"
#include <cstring>
#include <mutex>

// Reorganising pages: moving rows between pages, merging a sparse page with
// the next, splitting a page, and compacting one in place.

static constexpr size_t MAX_SLOTS_MERGE = 64;     // slot-directory allowance when merging pages

size_t HeapFile::move_rows(uint64_t from, uint64_t to, const RowPred& should_move,
                           std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    uint8_t* page = block_ptr(from);
    uint16_t n = page_header(page).slot_count;
    size_t count = 0;
    for (uint16_t i = 0; i < n; i++) {
        PageSlot se = page_slot(page, i);
        if (se.offset == 0) continue;
        const uint8_t* row = page + se.offset;
        if (!should_move(row, se.length) || !make_room(to, se.length)) continue;
        RecordPtr ptr = write_row(to, row, se.length);   // to != from: row stays in place
        moved.push_back({std::vector<uint8_t>(row, row + se.length), ptr});
        page_kill_row(page, i, se.length);
        count++;
    }
    return count;
}

bool HeapFile::merge_next(uint64_t block_id, std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    uint64_t next;
    {
        std::lock_guard<std::mutex> lock(heap_mutex_);
        if (!is_page(block_id)) return false;
        const uint8_t* page = block_ptr(block_id);
        next = page_next(page);
        if (next == 0 || next >= next_free_block_ || next == block_id) return false;
        // Only when this page is sparse and the next page's rows fit into it.
        size_t live_here = page_live_bytes(page), live_next = page_live_bytes(block_ptr(next));
        if (live_here > BLOCK_SIZE / 4) return false;
        if (live_next + live_here + 2 * MAX_SLOTS_MERGE * 4 > BLOCK_SIZE * 3 / 4) return false;
    }
    move_rows(next, block_id, [](const uint8_t*, uint16_t) { return true; }, moved);
    std::lock_guard<std::mutex> lock(heap_mutex_);
    uint8_t* np = block_ptr(next);
    if (page_live_bytes(np) != 0) return true;   // something didn't fit: keep both linked
    page_set_next(block_ptr(block_id), page_next(np));
    page_set_next(np, 0);
    if (next != write_block_ && !reuse_set_.count(next)) {   // emptied: reuse for appends
        reuse_set_.insert(next);
        reuse_pages_.push_back(next);
    }
    return true;
}

void HeapFile::compact_page(uint64_t block_id) {
    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> none;
    split_page_locked(block_id, [](const uint8_t*, uint16_t) { return false; }, none);
}

uint64_t HeapFile::split_page(uint64_t block_id, const RowPred& should_move,
                              std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    return split_page_locked(block_id, should_move, moved);
}

// Move rows matching should_move to a new page (only if any match), then
// compact the rest in place. With nothing to move this is just a compaction.
uint64_t HeapFile::split_page_locked(uint64_t block_id, const RowPred& should_move,
                                     std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    uint8_t* page = block_ptr(block_id);
    PageHeader phdr = page_header(page);

    struct Kept { uint16_t slot; std::vector<uint8_t> bytes; };
    std::vector<Kept> kept;
    uint64_t target = NULL_BLOCK;
    for (uint16_t i = 0; i < phdr.slot_count; i++) {
        PageSlot se = page_slot(page, i);
        if (se.offset == 0) continue;
        std::vector<uint8_t> row(page + se.offset, page + se.offset + se.length);
        if (should_move(row.data(), se.length)) {
            if (target == NULL_BLOCK) {
                target = new_page();   // the upper part follows this page in key order
                page_set_next(block_ptr(target), page_next(page));
                page_set_next(page, target);
            }
            RecordPtr p = write_row(target, row.data(), se.length);
            moved.push_back({std::move(row), p});
            set_page_slot(page, i, {0, 0});
        } else {
            kept.push_back({i, std::move(row)});
        }
    }

    // Compact the rows that stay towards the page end; slot numbers are unchanged.
    uint16_t free_end = BLOCK_SIZE;
    for (const auto& k : kept) {
        free_end = static_cast<uint16_t>(free_end - k.bytes.size());
        memcpy(page + free_end, k.bytes.data(), k.bytes.size());
        set_page_slot(page, k.slot, {free_end, static_cast<uint16_t>(k.bytes.size())});
    }
    phdr.free_end   = free_end;
    phdr.next       = page_next(page);   // may have been relinked above; don't write back the stale copy
    phdr.live_bytes = static_cast<uint16_t>(BLOCK_SIZE - free_end);
    phdr.dead_slots = static_cast<uint16_t>(phdr.slot_count - kept.size());
    set_page_header(page, phdr);
    return target;
}
