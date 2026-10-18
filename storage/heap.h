#pragma once
#include "index/chain.h"   // RecordPtr, BLOCK_SIZE
#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

class HeapFile {
public:
    static HeapFile create(const std::string& path);
    static HeapFile open(const std::string& path);
    ~HeapFile();

    HeapFile(HeapFile&& o) noexcept
        : fd_(o.fd_), base_(o.base_), mapped_size_(o.mapped_size_.load()),
          next_free_block_(o.next_free_block_), write_block_(o.write_block_),
          reuse_pages_(std::move(o.reuse_pages_)), reuse_set_(std::move(o.reuse_set_))
    { o.fd_ = -1; o.base_ = nullptr; o.mapped_size_ = 0; }

    HeapFile& operator=(HeapFile&&)      = delete;
    HeapFile(const HeapFile&)            = delete;
    HeapFile& operator=(const HeapFile&) = delete;

    RecordPtr insert(const uint8_t* data, uint16_t size);
    // Insert on the first of the given pages with room (keeps rows of
    // neighbouring keys together); if neither has room, start a fresh page.
    // With no valid page given, a plain insert.
    RecordPtr insert_near(uint64_t near_block, uint64_t alt_block, const uint8_t* data, uint16_t size);
    using RowPred = std::function<bool(const uint8_t* row, uint16_t size)>;

    // The live rows on a page, with their pointers.
    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> page_rows(uint64_t block_id) const;
    // Visit the live rows on a page in place (no copies). The bytes are valid
    // until the page is next written.
    void for_each_row(uint64_t block_id, const std::function<void(const uint8_t* row, uint16_t size)>& fn) const;
    // Every page's live-byte and dead-slot counts match its slot directory
    // (for tests).
    bool check_page_counts() const;
    // Next page in key order (a hint kept by splits and appends), or NULL_BLOCK.
    uint64_t next_page(uint64_t block_id) const;
    // A new empty page linked right after block_id in key order.
    uint64_t new_page_after(uint64_t block_id);
    // Bytes free on the page after compacting deleted rows.
    size_t   free_bytes(uint64_t block_id) const;
    // Move rows matching should_move from one page into another existing page
    // (as many as fit); returns how many moved.
    size_t   move_rows(uint64_t from, uint64_t to, const RowPred& should_move,
                       std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved);
    // If the page is sparse and its next page's rows fit into it, move them in
    // and unlink the emptied next page (recycled for appends). True if merged.
    bool     merge_next(uint64_t block_id, std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved);
    // True if a row of this size fits on the page.
    bool has_room(uint64_t block_id, uint16_t size) const;
    // Split a page: rows for which should_move(row) is true go to a new page
    // (returned, with each moved row's bytes and new pointer in moved); the
    // rows that stay keep their slot numbers, so their pointers stay valid.
    uint64_t split_page(uint64_t block_id, const RowPred& should_move,
                        std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved);
    bool read(RecordPtr ptr, std::vector<uint8_t>& out) const;
    // Hand the row's bytes to fn in place, without copying (valid during the
    // call). False if the record is gone.
    bool with_row(RecordPtr ptr, const std::function<void(const uint8_t* row, uint16_t size)>& fn) const;
    // Ask the kernel to start reading the pages these records live on
    // (heap_prefetch.cpp, #26), so a cold range scan's page faults overlap
    // instead of queueing; only for a few runs of consecutive pages, as each
    // run costs a system call even on a warm cache.
    void prefetch(std::vector<RecordPtr> ptrs) const;
    bool update(RecordPtr ptr, const uint8_t* data, uint16_t size);
    void remove(RecordPtr ptr);

    // Data blocks allocated (block 0 is the header).
    uint64_t block_count() const {
        std::lock_guard<std::mutex> l(heap_mutex_);
        return next_free_block_ > 1 ? next_free_block_ - 1 : 0;
    }

private:
    explicit HeapFile(int fd);
    void read_header();
    void flush_header();

    void     ensure_mapped      (uint64_t block_id);
    // For readers (no heap_mutex_ held): lock-free when block_id is already
    // mapped; otherwise grows the mapping under heap_mutex_, as writers do.
    void     ensure_mapped_const(uint64_t block_id) const {
        if ((block_id + 1) * BLOCK_SIZE <= mapped_size_.load(std::memory_order_acquire)) return;
        std::lock_guard<std::mutex> lock(heap_mutex_);
        const_cast<HeapFile*>(this)->ensure_mapped(block_id);
    }

    const uint8_t* row_ptr(RecordPtr ptr, uint16_t& size) const;
    uint8_t*       block_ptr(uint64_t block_id);
    const uint8_t* block_ptr(uint64_t block_id) const;

    uint64_t find_or_alloc_page(uint16_t needed_bytes);
    uint64_t new_page();                                       // caller holds heap_mutex_
    // Room for a row of this size, compacting away deleted rows' space if
    // that is what it takes (#19). Caller holds heap_mutex_.
    bool     make_room(uint64_t block_id, uint16_t size);
    void     compact_page(uint64_t block_id);                  // caller holds heap_mutex_
    uint64_t split_page_locked(uint64_t block_id, const RowPred& should_move,
                               std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved);
    RecordPtr write_row(uint64_t block_id, const uint8_t* data, uint16_t size);  // caller holds heap_mutex_

    int      fd_           = -1;
    void*    base_         = nullptr;
    std::atomic<size_t> mapped_size_{0};  // written under heap_mutex_, read lock-free
    uint64_t next_free_block_ = 1;
    uint64_t write_block_     = NULL_BLOCK;
    // Pages a delete left at least a quarter reclaimable: plain appends fill
    // these before starting a new page (in memory only; refilled as rows go).
    std::vector<uint64_t>        reuse_pages_;
    std::unordered_set<uint64_t> reuse_set_;
    mutable std::mutex heap_mutex_;
};
