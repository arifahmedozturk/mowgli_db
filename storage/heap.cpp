#include "storage/heap.h"
#include "storage/page.h"
#include <cstring>
#include <stdexcept>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

// Opening and closing a heap file, its header, and reading, updating and
// removing rows. Mapping the file is in heap_map.cpp, placing rows in
// heap_place.cpp, reorganising pages in heap_reorg.cpp, and the policy for a
// full page in heap_full_page.cpp; the page layout is storage/page.h.

static constexpr uint32_t HEAP_MAGIC = 0x48455050; // "HEPP"

#pragma pack(push, 1)
struct HeapFileHeader {
    uint32_t magic;
    uint32_t _pad;
    uint64_t next_free_block;
    uint64_t write_block;     // current insertion page (NULL_BLOCK if none)
};
#pragma pack(pop)

static_assert(sizeof(HeapFileHeader) == 24);

HeapFile::HeapFile(int fd) : fd_(fd) {
    base_ = mmap(nullptr, RESERVE, PROT_NONE,
                 MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (base_ == MAP_FAILED)
        throw std::runtime_error("HeapFile: virtual address reservation failed");
}

HeapFile::~HeapFile() {
    if (fd_ >= 0) {
        flush_header();
        if (base_ != nullptr && base_ != MAP_FAILED)
            msync(base_, mapped_size_.load(), MS_SYNC);
        ::close(fd_);
    }
    if (base_ != nullptr && base_ != MAP_FAILED)
        munmap(base_, RESERVE);
}

HeapFile HeapFile::create(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("cannot create heap file: " + path);
    HeapFile hf(fd);
    hf.flush_header();
    return hf;
}

HeapFile HeapFile::open(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) throw std::runtime_error("cannot open heap file: " + path);
    HeapFile hf(fd);
    hf.read_header();
    return hf;
}

uint8_t* HeapFile::block_ptr(uint64_t block_id) {
    ensure_mapped(block_id);
    return static_cast<uint8_t*>(base_) + static_cast<size_t>(block_id) * BLOCK_SIZE;
}

const uint8_t* HeapFile::block_ptr(uint64_t block_id) const {
    return static_cast<const uint8_t*>(base_) + static_cast<size_t>(block_id) * BLOCK_SIZE;
}

void HeapFile::flush_header() {
    uint8_t* frame = block_ptr(0);
    HeapFileHeader hdr{};
    hdr.magic           = HEAP_MAGIC;
    hdr.next_free_block = next_free_block_;
    hdr.write_block     = write_block_;
    memcpy(frame, &hdr, sizeof(HeapFileHeader));
}

void HeapFile::read_header() {
    ensure_mapped(0);
    const uint8_t* frame = block_ptr(0);
    HeapFileHeader hdr;
    memcpy(&hdr, frame, sizeof(HeapFileHeader));
    if (hdr.magic != HEAP_MAGIC)
        throw std::runtime_error("bad heap magic");
    next_free_block_ = hdr.next_free_block;
    write_block_     = hdr.write_block;
    if (next_free_block_ > 1)
        ensure_mapped(next_free_block_ - 1);
}

bool HeapFile::is_page(uint64_t block_id) const {
    return block_id != NULL_BLOCK && block_id != 0 && block_id < next_free_block_;
}

// ---- reading pages ----

std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> HeapFile::page_rows(uint64_t block_id) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> rows;
    if (!is_page(block_id)) return rows;
    ensure_mapped_const(block_id);
    const uint8_t* page = block_ptr(block_id);
    uint16_t n = page_header(page).slot_count;
    for (uint16_t i = 0; i < n; i++) {
        PageSlot se = page_slot(page, i);
        if (se.offset == 0) continue;
        rows.push_back({std::vector<uint8_t>(page + se.offset, page + se.offset + se.length),
                        RecordPtr{block_id, i}});
    }
    return rows;
}

void HeapFile::for_each_row(uint64_t block_id,
                            const std::function<void(const uint8_t* row, uint16_t size)>& fn) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    if (!is_page(block_id)) return;
    ensure_mapped_const(block_id);
    const uint8_t* page = block_ptr(block_id);
    uint16_t n = page_header(page).slot_count;
    for (uint16_t i = 0; i < n; i++) {
        PageSlot se = page_slot(page, i);
        if (se.offset != 0) fn(page + se.offset, se.length);
    }
}

uint64_t HeapFile::next_page(uint64_t block_id) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    if (!is_page(block_id)) return NULL_BLOCK;
    ensure_mapped_const(block_id);
    uint64_t n = page_next(block_ptr(block_id));
    return (n == 0 || n >= next_free_block_ || n == block_id) ? NULL_BLOCK : n;
}

size_t HeapFile::free_bytes(uint64_t block_id) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    if (!is_page(block_id)) return 0;
    ensure_mapped_const(block_id);
    return page_reclaimable_space(block_ptr(block_id));
}

bool HeapFile::has_room(uint64_t block_id, uint16_t size) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    if (!is_page(block_id)) return false;
    ensure_mapped_const(block_id);
    // Counting space a compaction would reclaim: a page with deleted rows is not "full".
    return page_reclaimable_space(block_ptr(block_id)) >= size + sizeof(PageSlot);
}

bool HeapFile::check_page_counts() const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    for (uint64_t b = 1; b < next_free_block_; b++) {
        ensure_mapped_const(b);
        const uint8_t* page = block_ptr(b);
        if (page_header(page).magic != PAGE_MAGIC) continue;
        if (!page_counts_match(page)) return false;
    }
    return true;
}

// ---- rows ----

// The live row at ptr in its page, or nullptr if it is gone.
const uint8_t* HeapFile::row_ptr(RecordPtr ptr, uint16_t& size) const {
    ensure_mapped_const(ptr.block_id);
    return page_row(block_ptr(ptr.block_id), ptr.slot, size);
}

bool HeapFile::read(RecordPtr ptr, std::vector<uint8_t>& out) const {
    uint16_t size;
    const uint8_t* row = row_ptr(ptr, size);
    if (!row) return false;
    out.assign(row, row + size);
    return true;
}

bool HeapFile::with_row(RecordPtr ptr, const std::function<void(const uint8_t*, uint16_t)>& fn) const {
    uint16_t size;
    const uint8_t* row = row_ptr(ptr, size);
    if (!row) return false;
    fn(row, size);
    return true;
}

bool HeapFile::update(RecordPtr ptr, const uint8_t* data, uint16_t size) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    uint8_t* page = block_ptr(ptr.block_id);
    uint16_t len;
    uint8_t* row = const_cast<uint8_t*>(page_row(page, ptr.slot, len));
    if (!row || len != size) return false;
    memcpy(row, data, size);
    return true;
}

void HeapFile::remove(RecordPtr ptr) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    uint8_t* page = block_ptr(ptr.block_id);
    uint16_t len;
    if (!page_row(page, ptr.slot, len)) return;   // already gone
    page_kill_row(page, ptr.slot, len);

    if (ptr.block_id != write_block_ && !reuse_set_.count(ptr.block_id) &&
        page_reclaimable_space(page) >= BLOCK_SIZE / 4) {
        reuse_set_.insert(ptr.block_id);
        reuse_pages_.push_back(ptr.block_id);
    }
}
