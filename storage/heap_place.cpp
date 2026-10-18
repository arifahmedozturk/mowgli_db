#include "storage/heap.h"
#include "storage/page.h"
#include <mutex>

// Placing new rows: on the current append page, a page with space freed by
// deletes, a neighbour's page, or a new page.

RecordPtr HeapFile::insert(const uint8_t* data, uint16_t size) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    return write_row(find_or_alloc_page(size), data, size);
}

RecordPtr HeapFile::insert_near(uint64_t near_block, uint64_t alt_block, const uint8_t* data, uint16_t size) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    if (!is_page(near_block) && !is_page(alt_block))
        return write_row(find_or_alloc_page(size), data, size);
    // A neighbour's page if the row fits; else a fresh page of its own, which
    // later neighbours of this key will find through this row's pointer.
    for (uint64_t b : {near_block, alt_block})
        if (is_page(b) && make_room(b, size)) return write_row(b, data, size);
    // A new page for the gap between the two neighbours' pages, linked in order.
    uint64_t page = new_page();
    if (is_page(near_block)) {
        page_set_next(block_ptr(page), page_next(block_ptr(near_block)));
        page_set_next(block_ptr(near_block), page);
    }
    return write_row(page, data, size);
}

uint64_t HeapFile::new_page_after(uint64_t block_id) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    uint64_t page = new_page();
    page_set_next(block_ptr(page), page_next(block_ptr(block_id)));
    page_set_next(block_ptr(block_id), page);
    return page;
}

uint64_t HeapFile::find_or_alloc_page(uint16_t needed_bytes) {
    if (write_block_ != NULL_BLOCK && make_room(write_block_, needed_bytes))
        return write_block_;

    // Pages with space freed by deletes, before growing the file.
    while (!reuse_pages_.empty()) {
        uint64_t b = reuse_pages_.back();
        reuse_pages_.pop_back();
        reuse_set_.erase(b);
        if (make_room(b, needed_bytes)) { write_block_ = b; flush_header(); return b; }
    }

    uint64_t prev = write_block_;
    write_block_ = new_page();
    flush_header();
    if (prev != NULL_BLOCK && page_next(block_ptr(prev)) == 0)
        page_set_next(block_ptr(prev), write_block_);   // appends of a sorted bulk load are in key order
    return write_block_;
}

uint64_t HeapFile::new_page() {
    uint64_t id = next_free_block_++;
    // Persist the allocation at once (#27): the header lives in the mapped
    // file, so this is a small memory write, and after a crash the reopened
    // heap never hands out a page that already holds rows.
    flush_header();
    page_init(block_ptr(id));
    return id;
}

bool HeapFile::make_room(uint64_t block_id, uint16_t size) {
    const uint8_t* page = block_ptr(block_id);
    size_t need = size + sizeof(PageSlot);
    if (page_free_space(page) >= need) return true;
    if (page_reclaimable_space(page) < need) return false;
    compact_page(block_id);
    return true;
}

RecordPtr HeapFile::write_row(uint64_t block_id, const uint8_t* data, uint16_t size) {
    return RecordPtr{block_id, page_write_row(block_ptr(block_id), data, size)};
}
