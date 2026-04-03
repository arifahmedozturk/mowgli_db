#pragma once
#include "index/chain.h"   // RecordPtr, BLOCK_SIZE
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

class HeapFile {
public:
    static HeapFile create(const std::string& path);
    static HeapFile open(const std::string& path);
    ~HeapFile();

    HeapFile(HeapFile&& o) noexcept
        : fd_(o.fd_), base_(o.base_), mapped_size_(o.mapped_size_),
          next_free_block_(o.next_free_block_), write_block_(o.write_block_)
    { o.fd_ = -1; o.base_ = nullptr; o.mapped_size_ = 0; }

    HeapFile& operator=(HeapFile&&)      = delete;
    HeapFile(const HeapFile&)            = delete;
    HeapFile& operator=(const HeapFile&) = delete;

    RecordPtr insert(const uint8_t* data, uint16_t size);
    bool read(RecordPtr ptr, std::vector<uint8_t>& out) const;
    bool update(RecordPtr ptr, const uint8_t* data, uint16_t size);
    void remove(RecordPtr ptr);

private:
    explicit HeapFile(int fd);
    void read_header();
    void flush_header();

    void     ensure_mapped      (uint64_t block_id);
    void     ensure_mapped_const(uint64_t block_id) const {
        const_cast<HeapFile*>(this)->ensure_mapped(block_id);
    }

    uint8_t*       block_ptr(uint64_t block_id);
    const uint8_t* block_ptr(uint64_t block_id) const;

    uint64_t find_or_alloc_page(uint16_t needed_bytes);

    int      fd_           = -1;
    void*    base_         = nullptr;
    size_t   mapped_size_  = 0;
    uint64_t next_free_block_ = 1;
    uint64_t write_block_     = NULL_BLOCK;
    mutable std::mutex heap_mutex_;
};
