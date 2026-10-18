#include "storage/buffer_pool.h"
#include <algorithm>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

BufferPool::BufferPool(size_t /*capacity*/) {
    base_ = mmap(nullptr, RESERVE_BYTES, PROT_NONE,
                 MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (base_ == MAP_FAILED)
        throw std::runtime_error("BufferPool: virtual address reservation failed");
}

BufferPool::~BufferPool() {
    if (base_ != nullptr && base_ != MAP_FAILED)
        munmap(base_, RESERVE_BYTES);
}

void BufferPool::ensure_mapped(uint64_t block_id, int fd) {
    size_t needed = (static_cast<size_t>(block_id) + 1) * BLOCK_SIZE;
    const size_t mapped = mapped_size_.load(std::memory_order_relaxed);   // grow_mutex_ is held
    if (needed <= mapped) return;
    // Mapping past the reservation would MAP_FIXED over unrelated memory.
    if (block_id >= RESERVE_BYTES / BLOCK_SIZE)
        throw std::runtime_error("BufferPool: file exceeds the " +
                                 std::to_string(RESERVE_BYTES >> 30) + " GiB mapping limit");

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
    size_t to_map = std::min(std::max(needed, file_size), RESERVE_BYTES);

    void* addr = static_cast<uint8_t*>(base_) + mapped;
    size_t len  = to_map - mapped;
    void* r = mmap(addr, len,
                   PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_FIXED,
                   fd, static_cast<off_t>(mapped));
    if (r == MAP_FAILED)
        throw std::runtime_error("BufferPool: MAP_FIXED failed");

    mapped_size_.store(to_map, std::memory_order_release);
}

void BufferPool::truncate(uint64_t n_blocks, int fd) {
    std::lock_guard<std::mutex> lg(grow_mutex_);
    size_t size = static_cast<size_t>(n_blocks) * BLOCK_SIZE;
    const size_t mapped = mapped_size_.load(std::memory_order_relaxed);
    if (size < mapped) {
        // Back to an inaccessible reservation, so nothing touches pages past EOF.
        void* addr = static_cast<uint8_t*>(base_) + size;
        if (mmap(addr, mapped - size, PROT_NONE,
                 MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | MAP_FIXED, -1, 0) == MAP_FAILED)
            throw std::runtime_error("BufferPool: unmapping the truncated tail failed");
        mapped_size_.store(size, std::memory_order_release);
    }
    if (ftruncate(fd, static_cast<off_t>(size)) < 0)
        throw std::runtime_error("BufferPool: ftruncate failed");
}

// Make sure block_id is mapped. The common case, an already mapped block,
// takes no lock (#77): mapped_size_ only grows while a pin can run (it shrinks
// only in truncate, which runs under the trie's exclusive latch).
void BufferPool::map_for(uint64_t block_id, int fd) {
    if ((static_cast<size_t>(block_id) + 1) * BLOCK_SIZE <= mapped_size_.load(std::memory_order_acquire)) return;
    std::lock_guard<std::mutex> lg(grow_mutex_);
    ensure_mapped(block_id, fd);
}

const uint8_t* BufferPool::pin_shared(uint64_t block_id, int fd) {
    if (pin_trace) pin_trace->push_back(block_id);
    map_for(block_id, fd);
    stripe(block_id).lock_shared();
    return static_cast<const uint8_t*>(base_) + static_cast<size_t>(block_id) * BLOCK_SIZE;
}

uint8_t* BufferPool::pin_exclusive(uint64_t block_id, int fd) {
    if (pin_trace) pin_trace->push_back(block_id);
    map_for(block_id, fd);
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
    if (size_t mapped = mapped_size_.load(); mapped > 0)
        msync(base_, mapped, MS_SYNC);
}

void BufferPool::invalidate(uint64_t /*block_id*/) {}   // OS manages page eviction

uint8_t* BufferPool::pin(uint64_t block_id, int fd) {
    return const_cast<uint8_t*>(pin_shared(block_id, fd));
}

void BufferPool::unpin(uint64_t block_id) {
    unpin_shared(block_id);
}
