#pragma once
#include "index/chain.h" // BLOCK_SIZE
#include <array>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <vector>

class BufferPool {
public:
    explicit BufferPool(size_t /*capacity — ignored, kept for API compat*/);
    ~BufferPool();

    BufferPool(const BufferPool&)            = delete;
    BufferPool& operator=(const BufferPool&) = delete;
    BufferPool(BufferPool&&)                 = delete;
    BufferPool& operator=(BufferPool&&)      = delete;

    const uint8_t* pin_shared   (uint64_t block_id, int fd);
    uint8_t*       pin_exclusive(uint64_t block_id, int fd);
    void           unpin_shared   (uint64_t block_id);
    void           unpin_exclusive(uint64_t block_id);

    void mark_dirty(uint64_t block_id);
    void flush_all(int fd);
    void invalidate(uint64_t block_id);

    uint8_t* pin  (uint64_t block_id, int fd);
    void     unpin(uint64_t block_id);

    // When set, every pin on this thread appends its block id (benchmarks use
    // this to count physical blocks touched per operation).
    static inline thread_local std::vector<uint64_t>* pin_trace = nullptr;

    size_t hits()   const { return 0; }
    size_t misses() const { return 0; }
    void   reset_stats() {}

private:
    // Virtual address reservation (PROT_NONE, no memory cost) that the file is
    // mapped into; blocks keep stable addresses, so it caps the file size.
    static constexpr size_t RESERVE_BYTES = 256ULL << 30;  // 256 GiB
    static constexpr size_t LATCH_STRIPES = 512;
    static constexpr size_t GROW_BLOCKS   = 1024;        // ftruncate in 8 MB chunks
    static constexpr size_t GROW_BYTES    = GROW_BLOCKS * BLOCK_SIZE;

    void*  base_        = nullptr;
    size_t mapped_size_ = 0;

    mutable std::mutex grow_mutex_;
    mutable std::array<std::shared_mutex, LATCH_STRIPES> latches_;

    std::shared_mutex& stripe(uint64_t block_id) const {
        return latches_[block_id % LATCH_STRIPES];
    }

    void ensure_mapped(uint64_t block_id, int fd);
};
