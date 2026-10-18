#include "storage/disk_manager.h"
#include "storage/pack_slot.h"
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>

// Opening and closing the file, raw block access and block allocation. The
// header lives in disk_manager_header.cpp, packed slots in _pack.cpp, chain
// rewrites in _update.cpp, the open-time rebuild in _rebuild.cpp and the
// compaction slide in _compact.cpp.

// ---- construction ----

DiskManager::DiskManager(int fd, uint64_t alloc_batch)
    : fd_(fd), alloc_batch_(alloc_batch) {}

std::unique_ptr<DiskManager> DiskManager::create(const std::string& path,
                                                   uint64_t alloc_batch) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("cannot create file: " + path);
    auto dm = std::unique_ptr<DiskManager>(new DiskManager(fd, alloc_batch));
    dm->flush_header();
    return dm;
}

std::unique_ptr<DiskManager> DiskManager::open(const std::string& path,
                                                uint64_t alloc_batch) {
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) throw std::runtime_error("cannot open file: " + path);
    auto dm = std::unique_ptr<DiskManager>(new DiskManager(fd, alloc_batch));
    dm->read_header();
    dm->rebuild_packed_state();
    return dm;
}

DiskManager::~DiskManager() {
    if (fd_ >= 0) {
        flush_header(/*spill=*/true);
        pool_.flush_all(fd_);
        ::close(fd_);
    }
}

void DiskManager::read_block(uint64_t block_id, uint8_t out[BLOCK_SIZE]) const {
    const uint8_t* frame = pool_.pin_shared(block_id, fd_);
    memcpy(out, frame, BLOCK_SIZE);
    pool_.unpin_shared(block_id);
}

void DiskManager::write_block(uint64_t block_id, const uint8_t buf[BLOCK_SIZE]) {
    uint8_t* frame = pool_.pin_exclusive(block_id, fd_);
    memcpy(frame, buf, BLOCK_SIZE);
    pool_.mark_dirty(block_id);
    pool_.unpin_exclusive(block_id);
}

const uint8_t* DiskManager::pin_chain_shared(uint64_t block_id) const {
    return pool_.pin_shared(block_id, fd_);
}
void DiskManager::unpin_chain_shared(uint64_t block_id) const {
    pool_.unpin_shared(block_id);
}

DiskManager::ChainBytes DiskManager::pin_chain_bytes(uint64_t chain_addr) const {
    uint8_t  slot = chain_addr_slot(chain_addr);
    uint64_t phys = chain_addr_phys(chain_addr);
    const uint8_t* frame = pool_.pin_shared(phys, fd_);
    if (slot == 0) return {frame, BLOCK_SIZE, phys};

    SlotEntry e = read_slot_entry(frame, slot);
    if (!e.live()) {
        pool_.unpin_shared(phys);
        throw std::runtime_error("pin_chain_bytes: slot is dead");
    }
    uint64_t fwd = stub_target(frame, e);
    if (fwd != NULL_BLOCK) {
        pool_.unpin_shared(phys);
        return pin_chain_bytes(fwd);  // moved to a larger slot or a dedicated block
    }
    return {frame + e.off, e.len, phys};
}
bool DiskManager::is_forwarded(uint64_t chain_addr) const {
    uint8_t slot = chain_addr_slot(chain_addr);
    if (slot == 0) return false;
    uint64_t phys = chain_addr_phys(chain_addr);
    const uint8_t* frame = pool_.pin_shared(phys, fd_);
    uint64_t fwd = stub_target(frame, read_slot_entry(frame, slot));
    pool_.unpin_shared(phys);
    return fwd != NULL_BLOCK;
}

DiskManager::ChainSlot DiskManager::pin_chain_slot(uint64_t chain_addr) {
    uint8_t  slot = chain_addr_slot(chain_addr);
    uint64_t phys = chain_addr_phys(chain_addr);
    uint8_t* frame = pool_.pin_exclusive(phys, fd_);
    if (slot == 0) return {frame, BLOCK_SIZE, phys};

    SlotEntry e = read_slot_entry(frame, slot);   // len is the slot's capacity
    if (!e.live()) {
        pool_.unpin_exclusive(phys);
        throw std::runtime_error("pin_chain_slot: slot is dead");
    }
    uint64_t fwd = stub_target(frame, e);
    if (fwd != NULL_BLOCK) {
        pool_.unpin_exclusive(phys);
        return pin_chain_slot(fwd);
    }
    return {frame + e.off, e.len, phys};
}

uint8_t* DiskManager::pin_chain_exclusive(uint64_t block_id) {
    return pool_.pin_exclusive(block_id, fd_);
}
void DiskManager::unpin_chain_exclusive(uint64_t block_id) {
    pool_.unpin_exclusive(block_id);
}

uint64_t DiskManager::alloc_block() {
    std::lock_guard<std::mutex> lock(header_mutex_);
    uint64_t id;
    if (!free_list_mem_.empty()) {
        id = free_list_mem_.back();
        free_list_mem_.pop_back();
        flush_header();   // the saved free list must never name a block in use (#56)
    } else {
        id = next_free_block_++;
        if (next_free_block_ > committed_ceil_) {
            committed_ceil_ = next_free_block_ + alloc_batch_ - 1;
            flush_header();
        }
    }
    return id;
}

void DiskManager::free_block(uint64_t block_id) {
    std::lock_guard<std::mutex> lock(header_mutex_);
    free_list_mem_.push_back(block_id);
}

void DiskManager::set_root_block(uint64_t id) {
    std::lock_guard<std::mutex> lock(header_mutex_);
    root_block_ = id;
    flush_header();
}

void DiskManager::set_key_count(uint64_t n) {
    std::lock_guard<std::mutex> lock(header_mutex_);
    key_count_ = n;
}

void DiskManager::update_chain(uint64_t chain_addr, const ChainData& chain) {
    update_chain_at(chain_addr, chain);
}

uint64_t DiskManager::write_chain(const ChainData& chain) {
    uint8_t buf[BLOCK_SIZE];
    if (!chain_encode(chain, buf))
        throw std::runtime_error("chain too large for one block");
    uint64_t id = alloc_block();
    write_block(id, buf);
    return id;
}

ChainData DiskManager::read_chain(uint64_t chain_addr) const {
    return read_chain_at(chain_addr);
}
