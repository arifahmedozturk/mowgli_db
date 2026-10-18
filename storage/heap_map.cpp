#include "storage/heap.h"
#include <algorithm>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

// Mapping the heap file into the address range reserved when it opened,
// growing the file in chunks.

static constexpr size_t HEAP_GROW_BLOCKS = 1024;   // ftruncate in 8 MB chunks

void HeapFile::ensure_mapped(uint64_t block_id) {
    size_t needed = (static_cast<size_t>(block_id) + 1) * BLOCK_SIZE;
    size_t mapped = mapped_size_.load(std::memory_order_relaxed);  // caller holds heap_mutex_
    if (needed <= mapped) return;
    // Mapping past the reservation would MAP_FIXED over unrelated memory.
    if (needed > RESERVE)
        throw std::runtime_error("HeapFile: file exceeds the " +
                                 std::to_string(RESERVE >> 30) + " GiB mapping limit");

    struct stat st;
    if (fstat(fd_, &st) < 0)
        throw std::runtime_error("HeapFile: fstat failed");
    size_t file_size = static_cast<size_t>(st.st_size);

    size_t grown_from = file_size;   // space added by this call, if any
    if (file_size < needed) {
        size_t grown = ((needed + HEAP_GROW_BLOCKS * BLOCK_SIZE - 1)
                        / (HEAP_GROW_BLOCKS * BLOCK_SIZE))
                       * (HEAP_GROW_BLOCKS * BLOCK_SIZE);
        if (ftruncate(fd_, static_cast<off_t>(grown)) < 0)
            throw std::runtime_error("HeapFile: ftruncate failed");
        file_size = grown;
    }

    size_t to_map = std::min(std::max(needed, file_size), RESERVE);
    void* addr = static_cast<uint8_t*>(base_) + mapped;
    void* r = mmap(addr, to_map - mapped,
                   PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_FIXED,
                   fd_, static_cast<off_t>(mapped));
    if (r == MAP_FAILED)
        throw std::runtime_error("HeapFile: MAP_FIXED failed");
#ifdef MADV_POPULATE_WRITE
    // Fault the new pages in now, in one call, rather than one page at a time
    // on first write: a page split's new page otherwise pays ~20 us (#68).
    // Best effort: older kernels reject the advice.
    if (file_size > grown_from && to_map > grown_from)
        madvise(static_cast<uint8_t*>(base_) + grown_from, to_map - grown_from, MADV_POPULATE_WRITE);
#endif
    mapped_size_.store(to_map, std::memory_order_release);
}
