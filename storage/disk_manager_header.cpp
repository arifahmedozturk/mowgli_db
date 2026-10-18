#include "storage/disk_manager.h"
#include <algorithm>
#include <cstring>
#include <mutex>
#include <stdexcept>

// The file header (block 0) and the free list's persistence.

static constexpr uint32_t FILE_MAGIC = 0x48545249; // "HTRI"

#pragma pack(push, 1)
struct FileHeader {
    uint32_t magic;
    uint32_t free_list_count;  // number of recycled IDs stored after this struct
    uint64_t root_block;
    uint64_t key_count;
    uint64_t next_free_block;
    uint64_t free_overflow;    // first overflow page of further free IDs, or NULL_BLOCK
};

// Free IDs beyond the header live in a chain of overflow pages, written on
// close. Each page is itself a free block, so it rejoins the free list on open.
struct FreePageHeader {
    uint32_t magic;
    uint32_t count;
    uint64_t next;             // next overflow page, or NULL_BLOCK
};
#pragma pack(pop)

static_assert(sizeof(FileHeader) == 40);
static_assert(sizeof(FreePageHeader) == 16);

static constexpr uint32_t FREE_PAGE_MAGIC = 0x46524545; // "FREE"

// Maximum free-block IDs stored inline in the header block after the fixed fields.
static constexpr size_t MAX_FREE_IN_HEADER = (BLOCK_SIZE - sizeof(FileHeader)) / sizeof(uint64_t);
static constexpr size_t MAX_FREE_IN_PAGE   = (BLOCK_SIZE - sizeof(FreePageHeader)) / sizeof(uint64_t);

void DiskManager::flush_header(bool spill) {
    size_t free_count = std::min(free_list_mem_.size(), MAX_FREE_IN_HEADER);

    // Overflow pages: take page blocks from the tail of the list itself and
    // fill each with the IDs before it, so every free ID is still recorded.
    uint64_t overflow = NULL_BLOCK;
    if (spill && free_list_mem_.size() > free_count) {
        std::vector<uint64_t> rest(free_list_mem_.begin() + free_count, free_list_mem_.end());
        while (!rest.empty()) {
            uint64_t page = rest.back();
            rest.pop_back();
            size_t n = std::min(rest.size(), MAX_FREE_IN_PAGE);
            FreePageHeader ph{FREE_PAGE_MAGIC, static_cast<uint32_t>(n), overflow};
            uint8_t* pf = pool_.pin_exclusive(page, fd_);
            memcpy(pf, &ph, sizeof(ph));
            memcpy(pf + sizeof(ph), rest.data() + rest.size() - n, n * sizeof(uint64_t));
            pool_.unpin_exclusive(page);
            rest.resize(rest.size() - n);
            overflow = page;
        }
    }

    uint8_t* frame = pool_.pin_exclusive(0, fd_);
    FileHeader hdr{};
    hdr.magic           = FILE_MAGIC;
    hdr.free_list_count = static_cast<uint32_t>(free_count);
    hdr.root_block      = root_block_;
    hdr.key_count       = key_count_;
    hdr.next_free_block = committed_ceil_;
    hdr.free_overflow   = overflow;
    memcpy(frame, &hdr, sizeof(FileHeader));
    memcpy(frame + sizeof(FileHeader),
           free_list_mem_.data(), free_count * sizeof(uint64_t));
    pool_.unpin_exclusive(0);
}

void DiskManager::read_header() {
    FileHeader hdr{};
    std::vector<uint64_t> free_ids;

    {
        const uint8_t* frame = pool_.pin_shared(0, fd_);
        memcpy(&hdr, frame, sizeof(FileHeader));
        if (hdr.magic != FILE_MAGIC) {
            pool_.unpin_shared(0);
            throw std::runtime_error("bad magic — not a heavy-trie file");
        }
        size_t count = std::min<size_t>(hdr.free_list_count, MAX_FREE_IN_HEADER);
        free_ids.resize(count);
        memcpy(free_ids.data(),
               frame + sizeof(FileHeader), count * sizeof(uint64_t));
        pool_.unpin_shared(0);
    }

    // Overflow pages: each page's IDs, then the page block itself, are free.
    for (uint64_t page = hdr.free_overflow; page != NULL_BLOCK;) {
        if (page == 0 || page >= hdr.next_free_block)
            throw std::runtime_error("corrupt free-list overflow page id");
        const uint8_t* pf = pool_.pin_shared(page, fd_);
        FreePageHeader ph;
        memcpy(&ph, pf, sizeof(ph));
        if (ph.magic != FREE_PAGE_MAGIC || ph.count > MAX_FREE_IN_PAGE) {
            pool_.unpin_shared(page);
            throw std::runtime_error("corrupt free-list overflow page");
        }
        size_t old = free_ids.size();
        free_ids.resize(old + ph.count);
        memcpy(free_ids.data() + old, pf + sizeof(ph), ph.count * sizeof(uint64_t));
        pool_.unpin_shared(page);
        free_ids.push_back(page);
        page = ph.next;
    }

    std::lock_guard<std::mutex> lock(header_mutex_);
    root_block_      = hdr.root_block;
    key_count_       = hdr.key_count;
    next_free_block_ = hdr.next_free_block;
    committed_ceil_  = hdr.next_free_block;
    free_list_mem_   = std::move(free_ids);
    // The overflow pages are now ordinary free blocks that may be reused, so
    // stop the header pointing at them (a crash then leaks, never corrupts).
    if (hdr.free_overflow != NULL_BLOCK) flush_header();
}
