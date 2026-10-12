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
};

struct SlotEntry {
    uint16_t offset; // 0 = deleted
    uint16_t length;
};
#pragma pack(pop)

static_assert(sizeof(HeapFileHeader) == 24);
static_assert(sizeof(PageHeader)     == 8);
static_assert(sizeof(SlotEntry)      == 4);

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

static uint16_t page_free_space(const uint8_t* page) {
    PageHeader hdr;
    memcpy(&hdr, page, sizeof(PageHeader));
    uint16_t dir_end = static_cast<uint16_t>(
        sizeof(PageHeader) + hdr.slot_count * sizeof(SlotEntry));
    return hdr.free_end > dir_end ? hdr.free_end - dir_end : 0;
}

uint64_t HeapFile::find_or_alloc_page(uint16_t needed_bytes) {
    uint16_t total_needed = needed_bytes + static_cast<uint16_t>(sizeof(SlotEntry));

    if (write_block_ != NULL_BLOCK) {
        const uint8_t* page = block_ptr(write_block_);
        if (page_free_space(page) >= total_needed)
            return write_block_;
    }

    write_block_ = new_page();
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
    uint16_t need = static_cast<uint16_t>(size + sizeof(SlotEntry));
    for (uint64_t b : {near_block, alt_block})
        if (usable(b) && page_free_space(block_ptr(b)) >= need) return write_row(b, data, size);
    return write_row(new_page(), data, size);
}

RecordPtr HeapFile::write_row(uint64_t block_id, const uint8_t* data, uint16_t size) {
    uint8_t* page = block_ptr(block_id);

    PageHeader phdr;
    memcpy(&phdr, page, sizeof(PageHeader));

    uint16_t new_free_end = phdr.free_end - size;
    memcpy(page + new_free_end, data, size);

    // Reuse a dead slot entry (deleted or split away) before growing the directory.
    uint16_t slot_index = phdr.slot_count;
    for (uint16_t i = 0; i < phdr.slot_count; i++) {
        SlotEntry old;
        memcpy(&old, page + sizeof(PageHeader) + i * sizeof(SlotEntry), sizeof(SlotEntry));
        if (old.offset == 0) { slot_index = i; break; }
    }
    SlotEntry se{new_free_end, size};
    memcpy(page + sizeof(PageHeader) + slot_index * sizeof(SlotEntry), &se, sizeof(SlotEntry));

    phdr.free_end = new_free_end;
    if (slot_index == phdr.slot_count) phdr.slot_count++;
    memcpy(page, &phdr, sizeof(PageHeader));

    return RecordPtr{block_id, slot_index};
}

bool HeapFile::has_room(uint64_t block_id, uint16_t size) const {
    std::lock_guard<std::mutex> lock(heap_mutex_);
    if (block_id == NULL_BLOCK || block_id == 0 || block_id >= next_free_block_) return false;
    ensure_mapped_const(block_id);
    return page_free_space(block_ptr(block_id)) >= size + sizeof(SlotEntry);
}

uint64_t HeapFile::split_page(uint64_t block_id, const RowPred& should_move,
                              std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    std::lock_guard<std::mutex> lock(heap_mutex_);
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
            if (target == NULL_BLOCK) target = new_page();
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
    phdr.free_end = free_end;
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

bool HeapFile::read(RecordPtr ptr, std::vector<uint8_t>& out) const {
    ensure_mapped_const(ptr.block_id);
    const uint8_t* page = block_ptr(ptr.block_id);

    PageHeader phdr;
    memcpy(&phdr, page, sizeof(PageHeader));
    if (ptr.slot >= phdr.slot_count) return false;

    SlotEntry se;
    memcpy(&se, page + sizeof(PageHeader) + ptr.slot * sizeof(SlotEntry), sizeof(SlotEntry));
    if (se.offset == 0) return false;

    out.assign(page + se.offset, page + se.offset + se.length);
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
    SlotEntry se{0, 0};
    memcpy(page + slot_offset, &se, sizeof(SlotEntry));
}
