#include "storage/heap.h"
#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static constexpr uint32_t HEAP_MAGIC = 0x48455050; // "HEPP"
static constexpr uint32_t PAGE_MAGIC = 0x48504147; // "HPAG"
static constexpr size_t   HEAP_RESERVE = 256ULL << 30; // virtual reservation (PROT_NONE); caps file size
static constexpr size_t   HEAP_GROW_BLOCKS = 1024;   // ftruncate in 8 MB chunks
static constexpr size_t   MAX_SLOTS_MERGE  = 64;     // slot-directory allowance when merging pages

#pragma pack(push, 1)
struct HeapFileHeader {
    uint32_t magic;
    uint32_t _pad;
    uint64_t next_free_block;
    uint64_t write_block;     // current insertion page (NULL_BLOCK if none)
};

struct PageHeader {
    uint32_t magic;
    uint16_t slot_count;
    uint16_t free_end;   // offset from page start where free space ends;
                         // records fill upward from BLOCK_SIZE
    uint64_t next;       // next page in key order (0 = unknown); a hint, never relied on for correctness
    uint16_t live_bytes; // bytes of live rows, so room checks need no directory scan (#74)
    uint16_t dead_slots; // directory entries of deleted or moved rows, reusable by write_row
};

struct SlotEntry {
    uint16_t offset; // 0 = deleted
    uint16_t length;
};
#pragma pack(pop)

static_assert(sizeof(HeapFileHeader) == 24);
static_assert(sizeof(PageHeader)     == 20);
static_assert(sizeof(SlotEntry)      == 4);

static uint64_t page_next(const uint8_t* page) {
    PageHeader h;
    memcpy(&h, page, sizeof(h));
    return h.next;
}

static void page_set_next(uint8_t* page, uint64_t next) {
    PageHeader h;
    memcpy(&h, page, sizeof(h));
    h.next = next;
    memcpy(page, &h, sizeof(h));
}

HeapFile::HeapFile(int fd) : fd_(fd) {
    base_ = mmap(nullptr, HEAP_RESERVE, PROT_NONE,
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
        munmap(base_, HEAP_RESERVE);
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

void HeapFile::ensure_mapped(uint64_t block_id) {
    size_t needed = (static_cast<size_t>(block_id) + 1) * BLOCK_SIZE;
    size_t mapped = mapped_size_.load(std::memory_order_relaxed);  // caller holds heap_mutex_
    if (needed <= mapped) return;
    // Mapping past the reservation would MAP_FIXED over unrelated memory.
    if (needed > HEAP_RESERVE)
        throw std::runtime_error("HeapFile: file exceeds the " +
                                 std::to_string(HEAP_RESERVE >> 30) + " GiB mapping limit");

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

    size_t to_map = std::min(std::max(needed, file_size), HEAP_RESERVE);
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

// Bytes held by live rows on a page (deleted rows' bytes stay in the page until
// it is compacted), kept in the header.
static size_t page_live_bytes(const uint8_t* page) {
    PageHeader hdr;
    memcpy(&hdr, page, sizeof(PageHeader));
    return hdr.live_bytes;
}

// A live row on the page became dead (deleted or moved away): update the header.
static void page_row_gone(uint8_t* page, uint16_t length) {
    PageHeader hdr;
    memcpy(&hdr, page, sizeof(PageHeader));
    hdr.live_bytes = static_cast<uint16_t>(hdr.live_bytes - length);
    hdr.dead_slots++;
    memcpy(page, &hdr, sizeof(PageHeader));
}

// Free bytes after compacting: everything but the header, directory and live rows.
static size_t page_reclaimable_space(const uint8_t* page) {
    PageHeader hdr;
    memcpy(&hdr, page, sizeof(PageHeader));
    size_t dir_end = sizeof(PageHeader) + hdr.slot_count * sizeof(SlotEntry);
    size_t used    = dir_end + page_live_bytes(page);
    return used < BLOCK_SIZE ? BLOCK_SIZE - used : 0;
}

static uint16_t page_free_space(const uint8_t* page) {
    PageHeader hdr;
    memcpy(&hdr, page, sizeof(PageHeader));
    uint16_t dir_end = static_cast<uint16_t>(
        sizeof(PageHeader) + hdr.slot_count * sizeof(SlotEntry));
    return hdr.free_end > dir_end ? hdr.free_end - dir_end : 0;
}

uint64_t HeapFile::find_or_alloc_page(uint16_t needed_bytes) {
    if (write_block_ != NULL_BLOCK && make_room(write_block_, needed_bytes))
        return write_block_;

    // Pages with space freed by deletes, before growing the file.
    while (!reuse_pages_.empty()) {
        uint64_t b = reuse_pages_.back();
        reuse_pages_.pop_back();
        reuse_set_.erase(b);
        if (make_room(b, needed_bytes)) { write_block_ = b; return b; }
    }

    uint64_t prev = write_block_;
    write_block_ = new_page();
    if (prev != NULL_BLOCK && page_next(block_ptr(prev)) == 0)
        page_set_next(block_ptr(prev), write_block_);   // appends of a sorted bulk load are in key order
    return write_block_;
}

uint64_t HeapFile::new_page() {
    uint64_t id = next_free_block_++;
    uint8_t* page = block_ptr(id);
    memset(page, 0, BLOCK_SIZE);
    PageHeader phdr{};
    phdr.magic      = PAGE_MAGIC;
    phdr.slot_count = 0;
    phdr.free_end   = BLOCK_SIZE;
    memcpy(page, &phdr, sizeof(PageHeader));
    return id;
}

RecordPtr HeapFile::insert(const uint8_t* data, uint16_t size) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    return write_row(find_or_alloc_page(size), data, size);
}

RecordPtr HeapFile::insert_near(uint64_t near_block, uint64_t alt_block, const uint8_t* data, uint16_t size) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    auto usable = [&](uint64_t b) { return b != NULL_BLOCK && b != 0 && b < next_free_block_; };
    if (!usable(near_block) && !usable(alt_block))
        return write_row(find_or_alloc_page(size), data, size);
    // A neighbour's page if the row fits; else a fresh page of its own, which
    // later neighbours of this key will find through this row's pointer.
    for (uint64_t b : {near_block, alt_block})
        if (usable(b) && make_room(b, size)) return write_row(b, data, size);
    // A new page for the gap between the two neighbours' pages, linked in order.
    uint64_t page = new_page();
    if (usable(near_block)) {
        page_set_next(block_ptr(page), page_next(block_ptr(near_block)));
        page_set_next(block_ptr(near_block), page);
    }
    return write_row(page, data, size);
}

RecordPtr HeapFile::write_row(uint64_t block_id, const uint8_t* data, uint16_t size) {
    uint8_t* page = block_ptr(block_id);

    PageHeader phdr;
    memcpy(&phdr, page, sizeof(PageHeader));

    uint16_t new_free_end = phdr.free_end - size;
    memcpy(page + new_free_end, data, size);

    // Reuse a dead slot entry (deleted or split away) before growing the directory;
    // only pages that have one are scanned.
    uint16_t slot_index = phdr.slot_count;
    for (uint16_t i = 0; phdr.dead_slots > 0 && i < phdr.slot_count; i++) {
        SlotEntry old;
        memcpy(&old, page + sizeof(PageHeader) + i * sizeof(SlotEntry), sizeof(SlotEntry));
        if (old.offset == 0) { slot_index = i; break; }
    }
    SlotEntry se{new_free_end, size};
    memcpy(page + sizeof(PageHeader) + slot_index * sizeof(SlotEntry), &se, sizeof(SlotEntry));

    phdr.free_end = new_free_end;
    phdr.live_bytes = static_cast<uint16_t>(phdr.live_bytes + size);
    if (slot_index == phdr.slot_count) phdr.slot_count++;
    else phdr.dead_slots--;
    memcpy(page, &phdr, sizeof(PageHeader));

    return RecordPtr{block_id, slot_index};
}

std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> HeapFile::page_rows(uint64_t block_id) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> rows;
    if (block_id == NULL_BLOCK || block_id == 0 || block_id >= next_free_block_) return rows;
    ensure_mapped_const(block_id);
    const uint8_t* page = block_ptr(block_id);
    PageHeader phdr;
    memcpy(&phdr, page, sizeof(PageHeader));
    for (uint16_t i = 0; i < phdr.slot_count; i++) {
        SlotEntry se;
        memcpy(&se, page + sizeof(PageHeader) + i * sizeof(SlotEntry), sizeof(SlotEntry));
        if (se.offset == 0) continue;
        rows.push_back({std::vector<uint8_t>(page + se.offset, page + se.offset + se.length),
                        RecordPtr{block_id, i}});
    }
    return rows;
}

void HeapFile::for_each_row(uint64_t block_id,
                            const std::function<void(const uint8_t* row, uint16_t size)>& fn) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    if (block_id == NULL_BLOCK || block_id == 0 || block_id >= next_free_block_) return;
    ensure_mapped_const(block_id);
    const uint8_t* page = block_ptr(block_id);
    PageHeader phdr;
    memcpy(&phdr, page, sizeof(PageHeader));
    for (uint16_t i = 0; i < phdr.slot_count; i++) {
        SlotEntry se;
        memcpy(&se, page + sizeof(PageHeader) + i * sizeof(SlotEntry), sizeof(SlotEntry));
        if (se.offset != 0) fn(page + se.offset, se.length);
    }
}

uint64_t HeapFile::next_page(uint64_t block_id) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    if (block_id == NULL_BLOCK || block_id == 0 || block_id >= next_free_block_) return NULL_BLOCK;
    ensure_mapped_const(block_id);
    uint64_t n = page_next(block_ptr(block_id));
    return (n == 0 || n >= next_free_block_ || n == block_id) ? NULL_BLOCK : n;
}

uint64_t HeapFile::new_page_after(uint64_t block_id) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    uint64_t page = new_page();
    page_set_next(block_ptr(page), page_next(block_ptr(block_id)));
    page_set_next(block_ptr(block_id), page);
    return page;
}

size_t HeapFile::free_bytes(uint64_t block_id) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    if (block_id == NULL_BLOCK || block_id == 0 || block_id >= next_free_block_) return 0;
    ensure_mapped_const(block_id);
    return page_reclaimable_space(block_ptr(block_id));
}

size_t HeapFile::move_rows(uint64_t from, uint64_t to, const RowPred& should_move,
                           std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    uint8_t* page = block_ptr(from);
    PageHeader phdr;
    memcpy(&phdr, page, sizeof(PageHeader));
    size_t count = 0;
    for (uint16_t i = 0; i < phdr.slot_count; i++) {
        uint8_t* entry = page + sizeof(PageHeader) + i * sizeof(SlotEntry);
        SlotEntry se;
        memcpy(&se, entry, sizeof(SlotEntry));
        if (se.offset == 0) continue;
        const uint8_t* row = page + se.offset;
        if (!should_move(row, se.length) || !make_room(to, se.length)) continue;
        RecordPtr ptr = write_row(to, row, se.length);   // to != from: row stays in place
        moved.push_back({std::vector<uint8_t>(row, row + se.length), ptr});
        SlotEntry dead{0, 0};
        memcpy(entry, &dead, sizeof(SlotEntry));
        page_row_gone(page, se.length);
        count++;
    }
    return count;
}

bool HeapFile::merge_next(uint64_t block_id, std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    uint64_t next;
    {
        std::lock_guard<std::mutex> lock(heap_mutex_);
        if (block_id == NULL_BLOCK || block_id == 0 || block_id >= next_free_block_) return false;
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

bool HeapFile::check_page_counts() const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    for (uint64_t b = 1; b < next_free_block_; b++) {
        ensure_mapped_const(b);
        const uint8_t* page = block_ptr(b);
        PageHeader hdr;
        memcpy(&hdr, page, sizeof(PageHeader));
        if (hdr.magic != PAGE_MAGIC) continue;
        size_t live = 0, dead = 0;
        for (uint16_t i = 0; i < hdr.slot_count; i++) {
            SlotEntry se;
            memcpy(&se, page + sizeof(PageHeader) + i * sizeof(SlotEntry), sizeof(SlotEntry));
            if (se.offset != 0) live += se.length; else dead++;
        }
        if (live != hdr.live_bytes || dead != hdr.dead_slots) return false;
    }
    return true;
}

bool HeapFile::has_room(uint64_t block_id, uint16_t size) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    if (block_id == NULL_BLOCK || block_id == 0 || block_id >= next_free_block_) return false;
    ensure_mapped_const(block_id);
    // Counting space a compaction would reclaim: a page with deleted rows is not "full".
    return page_reclaimable_space(block_ptr(block_id)) >= size + sizeof(SlotEntry);
}

bool HeapFile::make_room(uint64_t block_id, uint16_t size) {
    const uint8_t* page = block_ptr(block_id);
    size_t need = size + sizeof(SlotEntry);
    if (page_free_space(page) >= need) return true;
    if (page_reclaimable_space(page) < need) return false;
    compact_page(block_id);
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
    PageHeader phdr;
    memcpy(&phdr, page, sizeof(PageHeader));

    struct Kept { uint16_t slot; std::vector<uint8_t> bytes; };
    std::vector<Kept> kept;
    uint64_t target = NULL_BLOCK;
    for (uint16_t i = 0; i < phdr.slot_count; i++) {
        uint8_t* entry = page + sizeof(PageHeader) + i * sizeof(SlotEntry);
        SlotEntry se;
        memcpy(&se, entry, sizeof(SlotEntry));
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
            SlotEntry dead{0, 0};
            memcpy(entry, &dead, sizeof(SlotEntry));
        } else {
            kept.push_back({i, std::move(row)});
        }
    }

    // Compact the rows that stay towards the page end; slot numbers are unchanged.
    uint16_t free_end = BLOCK_SIZE;
    for (const auto& k : kept) {
        free_end = static_cast<uint16_t>(free_end - k.bytes.size());
        memcpy(page + free_end, k.bytes.data(), k.bytes.size());
        SlotEntry se{free_end, static_cast<uint16_t>(k.bytes.size())};
        memcpy(page + sizeof(PageHeader) + k.slot * sizeof(SlotEntry), &se, sizeof(SlotEntry));
    }
    phdr.free_end   = free_end;
    phdr.next       = page_next(page);   // may have been relinked above; don't write back the stale copy
    phdr.live_bytes = static_cast<uint16_t>(BLOCK_SIZE - free_end);
    phdr.dead_slots = static_cast<uint16_t>(phdr.slot_count - kept.size());
    memcpy(page, &phdr, sizeof(PageHeader));
    return target;
}

bool HeapFile::update(RecordPtr ptr, const uint8_t* data, uint16_t size) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    ensure_mapped(ptr.block_id);
    uint8_t* page = block_ptr(ptr.block_id);

    PageHeader phdr;
    memcpy(&phdr, page, sizeof(PageHeader));
    if (ptr.slot >= phdr.slot_count) return false;

    SlotEntry se;
    memcpy(&se, page + sizeof(PageHeader) + ptr.slot * sizeof(SlotEntry), sizeof(SlotEntry));
    if (se.offset == 0 || se.length != size) return false;

    memcpy(page + se.offset, data, size);
    return true;
}

// The live row at ptr in its page, or nullptr if it is gone.
const uint8_t* HeapFile::row_ptr(RecordPtr ptr, uint16_t& size) const {
    ensure_mapped_const(ptr.block_id);
    const uint8_t* page = block_ptr(ptr.block_id);

    PageHeader phdr;
    memcpy(&phdr, page, sizeof(PageHeader));
    if (ptr.slot >= phdr.slot_count) return nullptr;

    SlotEntry se;
    memcpy(&se, page + sizeof(PageHeader) + ptr.slot * sizeof(SlotEntry), sizeof(SlotEntry));
    if (se.offset == 0) return nullptr;
    size = se.length;
    return page + se.offset;
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

void HeapFile::remove(RecordPtr ptr) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    ensure_mapped(ptr.block_id);
    uint8_t* page = block_ptr(ptr.block_id);

    PageHeader phdr;
    memcpy(&phdr, page, sizeof(PageHeader));
    if (ptr.slot >= phdr.slot_count) return;

    uint16_t slot_offset = static_cast<uint16_t>(
        sizeof(PageHeader) + ptr.slot * sizeof(SlotEntry));
    SlotEntry old;
    memcpy(&old, page + slot_offset, sizeof(SlotEntry));
    if (old.offset == 0) return;   // already gone
    SlotEntry se{0, 0};
    memcpy(page + slot_offset, &se, sizeof(SlotEntry));
    page_row_gone(page, old.length);

    if (ptr.block_id != write_block_ && !reuse_set_.count(ptr.block_id) &&
        page_reclaimable_space(page) >= BLOCK_SIZE / 4) {
        reuse_set_.insert(ptr.block_id);
        reuse_pages_.push_back(ptr.block_id);
    }
}
