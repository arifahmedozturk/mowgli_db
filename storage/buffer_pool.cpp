#include "storage/buffer_pool.h"
#include <algorithm>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

BufferPool::BufferPool(size_t /*capacity*/) {
    base_ = mmap(nullptr, RESERVE_BYTES, PROT_NONE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base_ == MAP_FAILED)
        throw std::runtime_error("BufferPool: virtual address reservation failed");
}

BufferPool::~BufferPool() {
    if (base_ != nullptr && base_ != MAP_FAILED)
        munmap(base_, RESERVE_BYTES);
}

void BufferPool::ensure_mapped(uint64_t block_id, int fd) {
    size_t needed = (static_cast<size_t>(block_id) + 1) * BLOCK_SIZE;
    if (needed <= mapped_size_) return;

    // Check the actual file size so we never ftruncate backwards.
    struct stat st;
    if (fstat(fd, &st) < 0)
        throw std::runtime_error("BufferPool: fstat failed");
    size_t file_size = static_cast<size_t>(st.st_size);

    if (file_size < needed) {
        // Pre-grow by GROW_BLOCKS blocks at a time to amortize ftruncate cost.
        size_t grown = ((needed + GROW_BYTES - 1) / GROW_BYTES) * GROW_BYTES;
        if (ftruncate(fd, static_cast<off_t>(grown)) < 0)
            throw std::runtime_error("BufferPool: ftruncate failed");
        file_size = grown;
    }

    // Map in one shot: everything from mapped_size_ up to the greater of
    // needed and the actual file size, so subsequent blocks don't each
    // trigger a separate MAP_FIXED call.
    size_t to_map = std::max(needed, file_size);

    void* addr = static_cast<uint8_t*>(base_) + mapped_size_;
    size_t len  = to_map - mapped_size_;
    void* r = mmap(addr, len,
                   PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_FIXED,
                   fd, static_cast<off_t>(mapped_size_));
    if (r == MAP_FAILED)
        throw std::runtime_error("BufferPool: MAP_FIXED failed");

    mapped_size_ = to_map;
}

const uint8_t* BufferPool::pin_shared(uint64_t block_id, int fd) {
    { std::lock_guard<std::mutex> lg(grow_mutex_); ensure_mapped(block_id, fd); }
    stripe(block_id).lock_shared();
    return static_cast<const uint8_t*>(base_) + static_cast<size_t>(block_id) * BLOCK_SIZE;
}

uint8_t* BufferPool::pin_exclusive(uint64_t block_id, int fd) {
    { std::lock_guard<std::mutex> lg(grow_mutex_); ensure_mapped(block_id, fd); }
    stripe(block_id).lock();
    return static_cast<uint8_t*>(base_) + static_cast<size_t>(block_id) * BLOCK_SIZE;
}

void BufferPool::unpin_shared(uint64_t block_id) {
    stripe(block_id).unlock_shared();
}

void BufferPool::unpin_exclusive(uint64_t block_id) {
    stripe(block_id).unlock();
}

void BufferPool::mark_dirty(uint64_t /*block_id*/) {}   // MAP_SHARED: writes are already in the file

void BufferPool::flush_all(int /*fd*/) {
    std::lock_guard<std::mutex> lg(grow_mutex_);
    if (mapped_size_ > 0)
        msync(base_, mapped_size_, MS_SYNC);
}

void BufferPool::invalidate(uint64_t /*block_id*/) {}   // OS manages page eviction

uint8_t* BufferPool::pin(uint64_t block_id, int fd) {
    return const_cast<uint8_t*>(pin_shared(block_id, fd));
}

void BufferPool::unpin(uint64_t block_id) {
    unpin_shared(block_id);
}
