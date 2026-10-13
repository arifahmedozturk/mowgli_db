#include "storage/disk_manager.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_set>
#include <fcntl.h>
#include <unistd.h>

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

    uint16_t off, len;
    memcpy(&off, frame + PACK_DIR_OFFSET + (slot - 1) * 4,     2);
    memcpy(&len, frame + PACK_DIR_OFFSET + (slot - 1) * 4 + 2, 2);
    if (off == 0 || len == 0) {
        pool_.unpin_shared(phys);
        throw std::runtime_error("pin_chain_bytes: slot is dead");
    }
    uint16_t tag = 0;
    if (len >= 2) memcpy(&tag, frame + off, 2);
    if (tag == FORWARD_MAGIC && len >= 10) {
        uint64_t fwd;
        memcpy(&fwd, frame + off + 2, 8);
        pool_.unpin_shared(phys);
        return pin_chain_bytes(fwd);  // moved to a larger slot or a dedicated block
    }
    return {frame + off, len, phys};
}
DiskManager::ChainSlot DiskManager::pin_chain_slot(uint64_t chain_addr) {
    uint8_t  slot = chain_addr_slot(chain_addr);
    uint64_t phys = chain_addr_phys(chain_addr);
    uint8_t* frame = pool_.pin_exclusive(phys, fd_);
    if (slot == 0) return {frame, BLOCK_SIZE, phys};

    uint16_t off, len;   // len is the slot's capacity
    memcpy(&off, frame + PACK_DIR_OFFSET + (slot - 1) * 4,     2);
    memcpy(&len, frame + PACK_DIR_OFFSET + (slot - 1) * 4 + 2, 2);
    if (off == 0 || len == 0) {
        pool_.unpin_exclusive(phys);
        throw std::runtime_error("pin_chain_slot: slot is dead");
    }
    uint16_t tag = 0;
    if (len >= 2) memcpy(&tag, frame + off, 2);
    if (tag == FORWARD_MAGIC && len >= 10) {
        uint64_t fwd;
        memcpy(&fwd, frame + off + 2, 8);
        pool_.unpin_exclusive(phys);
        return pin_chain_slot(fwd);
    }
    return {frame + off, len, phys};
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

void DiskManager::compact_blocks(const std::vector<uint64_t>& live) {
    std::lock_guard<std::mutex> lp(pack_mutex_);
    std::lock_guard<std::mutex> lh(header_mutex_);

    // Ascending order: each target is free or was already moved down.
    std::unordered_map<uint64_t, uint64_t> moved;
    moved.reserve(live.size());
    uint8_t buf[BLOCK_SIZE];
    for (size_t i = 0; i < live.size(); i++) {
        uint64_t from = live[i], to = compacted_phys(i);
        assert(to <= from && (i == 0 || live[i - 1] < from));
        moved[from] = to;
        if (from == to) continue;
        read_block(from, buf);
        write_block(to, buf);
    }

    std::unordered_map<uint64_t, PackedBlockState> packed;
    packed.reserve(packed_blocks_.size());
    std::vector<uint64_t> candidates;
    for (auto& [phys, st] : packed_blocks_) {
        auto it = moved.find(phys);
        if (it == moved.end()) continue;   // garbage left behind
        packed[it->second] = st;
        if (st.in_candidates) candidates.push_back(it->second);
    }
    packed_blocks_   = std::move(packed);
    pack_candidates_ = std::move(candidates);

    free_list_mem_.clear();
    next_free_block_ = committed_ceil_ = compacted_phys(live.size());
    flush_header();
    pool_.truncate(next_free_block_, fd_);
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

void DiskManager::rebuild_packed_state() {
    uint64_t n_blocks;
    std::unordered_set<uint64_t> freed;
    {
        std::lock_guard<std::mutex> lh(header_mutex_);
        n_blocks = next_free_block_;
        freed.insert(free_list_mem_.begin(), free_list_mem_.end());
    }
    for (uint64_t blk = 1; blk < n_blocks; blk++) {
        if (freed.count(blk)) continue;
        const uint8_t* frame = pool_.pin_shared(blk, fd_);
        uint32_t magic;
        memcpy(&magic, frame, 4);
        pool_.unpin_shared(blk);
        if (magic == PACK_BLOCK_MAGIC)
            rebuild_packed_block(blk);
    }
}

void DiskManager::rebuild_packed_block(uint64_t phys) {
    const uint8_t* frame = pool_.pin_shared(phys, fd_);

    uint16_t num_slots;
    memcpy(&num_slots, frame + 4, 2);
    num_slots = std::min<uint16_t>(num_slots, static_cast<uint16_t>(PACK_MAX_SLOTS));

    PackedBlockState st{};
    st.num_slots = static_cast<uint8_t>(num_slots);

    for (uint8_t i = 0; i < st.num_slots; i++) {
        size_t de = PACK_DIR_OFFSET + i * 4;
        uint16_t off, len;
        memcpy(&off, frame + de,     2);
        memcpy(&len, frame + de + 2, 2);
        st.offsets[i] = off;
        st.lengths[i] = len;

        if (off == 0) continue;
        if (len == 0) {                       // dead: its region still occupies [off, next slot)
            uint16_t start = static_cast<uint16_t>(off - PACK_DATA_OFFSET);
            if (start > st.data_end) st.data_end = start;
            continue;
        }

        // Determine if slot holds a live chain or a forwarding stub.
        if (len >= 2) {
            uint16_t tag;
            memcpy(&tag, frame + off, 2);
            if (tag != FORWARD_MAGIC)
                st.live_count++;
        } else {
            st.live_count++;
        }

        uint16_t slot_end = static_cast<uint16_t>(off + len - PACK_DATA_OFFSET);
        if (slot_end > st.data_end) st.data_end = slot_end;
    }
    pool_.unpin_shared(phys);

    std::lock_guard<std::mutex> lp(pack_mutex_);
    packed_blocks_[phys] = std::move(st);
    auto& s2 = packed_blocks_[phys];
    if (s2.fits(PACK_MIN_SLOT)) {
        s2.in_candidates = true;
        pack_candidates_.push_back(phys);
    }
}

uint64_t DiskManager::alloc_chain_slot(const ChainData& chain, uint64_t hint_phys) {
    uint8_t tmp[BLOCK_SIZE];
    size_t enc = chain_encode_slice(chain, tmp, sizeof(tmp));
    if (enc == 0)
        throw std::runtime_error("chain_encode_slice: chain too large");
    return place_bytes(tmp, enc, hint_phys);
}

uint64_t DiskManager::place_bytes(const uint8_t* bytes, size_t enc, uint64_t hint_phys) {
    // Large chains and buckets get their own block.
    if (enc > PACK_THRESHOLD) {
        uint64_t id = alloc_block();
        uint8_t full[BLOCK_SIZE]{};
        memcpy(full, bytes, enc);
        write_block(id, full);
        return id; // slot bits = 0
    }

    // Reserve the size class, not just enc bytes, so the chain can grow in place.
    const uint16_t cap = static_cast<uint16_t>(pack_slot_class(enc));
    auto has_room = [&](const PackedBlockState& s, size_t need) { return s.fits(need); };

    // --- find or allocate a packed block with enough room ---
    std::unique_lock<std::mutex> lp(pack_mutex_);

    uint64_t target = NULL_BLOCK;
    if (hint_phys != NULL_BLOCK && hint_phys != 0) {
        auto it = packed_blocks_.find(hint_phys);
        if (it != packed_blocks_.end() && has_room(it->second, cap))
            target = hint_phys;
    }

    // Most recent candidates first. Blocks with no room for even the smallest
    // class are dropped; the scan is bounded so allocation stays cheap.
    if (target == NULL_BLOCK) {
        size_t looked = 0;
        for (size_t i = pack_candidates_.size(); i-- > 0 && looked < 16;) {
            uint64_t cand = pack_candidates_[i];
            auto it = packed_blocks_.find(cand);
            if (it == packed_blocks_.end() || !has_room(it->second, PACK_MIN_SLOT)) {
                if (it != packed_blocks_.end()) it->second.in_candidates = false;
                pack_candidates_.erase(pack_candidates_.begin() + static_cast<ptrdiff_t>(i));
                continue;
            }
            looked++;
            if (has_room(it->second, cap)) { target = cand; break; }
        }
    }

    if (target == NULL_BLOCK) {
        // Allocate a new packed block — release pack lock while calling alloc_block.
        lp.unlock();
        uint64_t new_phys = alloc_block();
        lp.lock();
        // Write the packed block header (magic + num_slots=0, zeroed directory).
        {
            uint8_t* frame = pool_.pin_exclusive(new_phys, fd_);
            memset(frame, 0, BLOCK_SIZE);
            uint32_t magic = PACK_BLOCK_MAGIC;
            memcpy(frame, &magic, 4);
            pool_.unpin_exclusive(new_phys);
        }
        packed_blocks_[new_phys] = PackedBlockState{};
        packed_blocks_[new_phys].in_candidates = true;
        pack_candidates_.push_back(new_phys);
        target = new_phys;
    }

    PackedBlockState& st = packed_blocks_[target];

    // Reuse the best-fitting dead slot (its whole region), else append at the tail.
    int dead = st.dead_fit(cap);
    if (dead >= 0) {
        uint8_t  slot   = static_cast<uint8_t>(dead + 1);
        uint16_t off    = st.offsets[dead];
        uint16_t region = st.capacity(static_cast<size_t>(dead));
        uint8_t* frame  = pool_.pin_exclusive(target, fd_);
        size_t de = PACK_DIR_OFFSET + dead * 4;
        memcpy(frame + de,     &off,    2);
        memcpy(frame + de + 2, &region, 2);
        memcpy(frame + off, bytes, enc);
        memset(frame + off + enc, 0, region - enc);
        pool_.unpin_exclusive(target);
        st.lengths[dead] = region;
        st.live_count++;
        return make_chain_addr(target, slot);
    }

    uint8_t  new_slot     = static_cast<uint8_t>(st.num_slots + 1); // 1-indexed
    uint16_t abs_offset   = static_cast<uint16_t>(PACK_DATA_OFFSET + st.data_end);

    // The directory's len field records the slot's capacity (its size class),
    // so in-place updates stay within bounds; decoding stops at the encoded size.
    {
        uint8_t* frame = pool_.pin_exclusive(target, fd_);
        size_t de = PACK_DIR_OFFSET + (new_slot - 1) * 4;
        memcpy(frame + de,     &abs_offset, 2);
        memcpy(frame + de + 2, &cap,        2);
        uint16_t ns = new_slot;
        memcpy(frame + 4, &ns, 2);
        // Write the encoded chain data; zero the remaining reserved bytes.
        memcpy(frame + abs_offset, bytes, enc);
        if (enc < cap)
            memset(frame + abs_offset + enc, 0, cap - enc);
        pool_.unpin_exclusive(target);
    }

    st.offsets[new_slot - 1] = abs_offset;
    st.lengths[new_slot - 1] = cap;   // capacity, not current size
    st.num_slots              = new_slot;
    st.data_end               = static_cast<uint16_t>(st.data_end + cap);
    st.live_count++;

    // Remove from candidates if no longer useful.
    if (!has_room(st, PACK_MIN_SLOT)) {
        auto it = std::find(pack_candidates_.begin(), pack_candidates_.end(), target);
        if (it != pack_candidates_.end()) pack_candidates_.erase(it);
        st.in_candidates = false;
    }

    return make_chain_addr(target, new_slot);
}

void DiskManager::free_chain_slot(uint64_t chain_addr) {
    if (chain_addr == NULL_BLOCK) return;
    if (chain_addr_slot(chain_addr) == 0) {
        free_block(chain_addr_phys(chain_addr));
        return;
    }
    // A stub's target holds the chain's data (never another stub): free it too.
    uint64_t forward_addr = free_packed_slot(chain_addr);
    if (forward_addr != NULL_BLOCK) free_chain_slot(forward_addr);
}

uint64_t DiskManager::free_packed_slot(uint64_t chain_addr) {
    uint8_t  slot = chain_addr_slot(chain_addr);
    uint64_t phys = chain_addr_phys(chain_addr);

    std::lock_guard<std::mutex> lp(pack_mutex_);
    auto it = packed_blocks_.find(phys);
    if (it == packed_blocks_.end()) return NULL_BLOCK; // already gone

    PackedBlockState& st = it->second;
    uint16_t off = st.offsets[slot - 1];
    uint16_t len = st.lengths[slot - 1];
    if (len == 0) return NULL_BLOCK; // already dead

    uint64_t forward_addr = NULL_BLOCK;
    bool     is_live      = true;

    // Read the slot under exclusive latch to check for a forwarding stub,
    // then immediately mark the directory entry dead.
    {
        uint8_t* frame = pool_.pin_exclusive(phys, fd_);
        if (len >= 2) {
            uint16_t tag;
            memcpy(&tag, frame + off, 2);
            if (tag == FORWARD_MAGIC && len >= 10) {
                memcpy(&forward_addr, frame + off + 2, 8);
                is_live = false;
            }
        }
        // Dead = length 0; the offset stays so the region can be reused.
        uint16_t zero = 0;
        size_t de = PACK_DIR_OFFSET + (slot - 1) * 4;
        memcpy(frame + de + 2, &zero, 2);
        pool_.unpin_exclusive(phys);
    }

    st.lengths[slot - 1] = 0;
    if (is_live) st.live_count--;

    // Release the physical packed block if every slot is dead.
    bool all_dead = true;
    for (uint8_t i = 0; i < st.num_slots; i++)
        if (st.lengths[i] > 0) { all_dead = false; break; }

    if (all_dead) {
        packed_blocks_.erase(it);
        auto ic = std::find(pack_candidates_.begin(), pack_candidates_.end(), phys);
        if (ic != pack_candidates_.end()) pack_candidates_.erase(ic);
        free_block(phys);
    } else if (!st.in_candidates) {
        // The freed region makes this block a place to allocate again.
        st.in_candidates = true;
        pack_candidates_.push_back(phys);
    }
    return forward_addr;
}

ChainData DiskManager::read_chain_at(uint64_t chain_addr) const {
    uint8_t  slot = chain_addr_slot(chain_addr);
    uint64_t phys = chain_addr_phys(chain_addr);

    if (slot == 0) {
        // Dedicated block.
        uint8_t buf[BLOCK_SIZE];
        read_block(phys, buf);

        ChainData out;
        if (!chain_decode(buf, out))
            throw std::runtime_error("read_chain_at: invalid dedicated chain");
        return out;
    }

    // Packed block: read directory entry, then the chain slice.
    const uint8_t* frame = pool_.pin_shared(phys, fd_);

    size_t   de  = PACK_DIR_OFFSET + (slot - 1) * 4;
    uint16_t off, len;
    memcpy(&off, frame + de,     2);
    memcpy(&len, frame + de + 2, 2);

    if (off == 0 || len == 0) {
        pool_.unpin_shared(phys);
        throw std::runtime_error("read_chain_at: slot is dead");
    }

    // Check for forwarding stub.
    uint16_t tag = 0;
    if (len >= 2) memcpy(&tag, frame + off, 2);

    if (tag == FORWARD_MAGIC && len >= 10) {
        uint64_t fwd;
        memcpy(&fwd, frame + off + 2, 8);
        pool_.unpin_shared(phys);
        return read_chain_at(fwd); // one hop: stubs always point straight at the data
    }

    ChainData out;
    bool ok = chain_decode_slice(frame + off, len, out);
    pool_.unpin_shared(phys);
    if (!ok) throw std::runtime_error("read_chain_at: invalid packed chain");
    return out;
}

uint64_t DiskManager::update_chain_at(uint64_t chain_addr, const ChainData& chain) {
    uint8_t  slot = chain_addr_slot(chain_addr);
    uint64_t phys = chain_addr_phys(chain_addr);

    uint8_t tmp[BLOCK_SIZE];
    size_t enc = chain_encode_slice(chain, tmp, sizeof(tmp));
    if (enc == 0) throw std::runtime_error("update_chain_at: chain too large");

    if (slot == 0) {
        // Dedicated block: anything that encodes fits.
        uint8_t full[BLOCK_SIZE]{};
        memcpy(full, tmp, enc);
        write_block(phys, full);
        return chain_addr;
    }

    // --- Packed slot ---
    auto slot_info = [&](uint64_t addr, uint16_t& off, uint16_t& cap) {
        std::lock_guard<std::mutex> lp(pack_mutex_);
        auto it = packed_blocks_.find(chain_addr_phys(addr));
        if (it == packed_blocks_.end())
            throw std::runtime_error("update_chain_at: unknown packed block");
        off = it->second.offsets[chain_addr_slot(addr) - 1];
        cap = it->second.lengths[chain_addr_slot(addr) - 1];
    };
    auto write_in_place = [&](uint64_t addr, uint16_t off) {
        uint64_t p = chain_addr_phys(addr);
        uint8_t* frame = pool_.pin_exclusive(p, fd_);
        memcpy(frame + off, tmp, enc);
        pool_.unpin_exclusive(p);
    };

    uint16_t old_off, old_cap;
    slot_info(chain_addr, old_off, old_cap);

    // Already forwarded: update the chain where it lives now.
    uint64_t fwd = NULL_BLOCK;
    {
        const uint8_t* frame = pool_.pin_shared(phys, fd_);
        uint16_t tag = 0;
        if (old_cap >= 2) memcpy(&tag, frame + old_off, 2);
        if (tag == FORWARD_MAGIC && old_cap >= 10) memcpy(&fwd, frame + old_off + 2, 8);
        pool_.unpin_shared(phys);
    }
    if (fwd != NULL_BLOCK) {
        if (chain_addr_slot(fwd) == 0) {           // dedicated: always fits
            uint8_t full[BLOCK_SIZE]{};
            memcpy(full, tmp, enc);
            write_block(chain_addr_phys(fwd), full);
            return fwd;
        }
        uint16_t f_off, f_cap;
        slot_info(fwd, f_off, f_cap);
        if (enc <= f_cap) { write_in_place(fwd, f_off); return fwd; }
        // Outgrew the forwarded slot too: move again, repoint the stub, free the old copy.
        uint64_t moved = place_bytes(tmp, enc, chain_addr_phys(fwd));
        {
            uint8_t* frame = pool_.pin_exclusive(phys, fd_);
            memcpy(frame + old_off + 2, &moved, 8);
            pool_.unpin_exclusive(phys);
        }
        free_chain_slot(fwd);
        return moved;
    }

    if (enc <= old_cap) {
        // Fits within the slot's reserved capacity — write in-place.
        // Do NOT change the directory len (it records capacity, not current size).
        write_in_place(chain_addr, old_off);
        return chain_addr;
    }

    // Doesn't fit — move to a larger slot (or a dedicated block) and leave a
    // forwarding stub, so parent pointers to chain_addr stay valid.
    uint64_t moved = place_bytes(tmp, enc, phys);
    {
        uint8_t* frame = pool_.pin_exclusive(phys, fd_);
        uint16_t fwd_tag  = FORWARD_MAGIC;
        uint16_t stub_len = 10;       // 2 (tag) + 8 (addr)
        size_t de = PACK_DIR_OFFSET + (slot - 1) * 4;
        memcpy(frame + de + 2, &stub_len, 2);
        memcpy(frame + old_off,     &fwd_tag, 2);
        memcpy(frame + old_off + 2, &moved,   8);
        pool_.unpin_exclusive(phys);
    }
    {
        std::lock_guard<std::mutex> lp(pack_mutex_);
        auto it = packed_blocks_.find(phys);
        if (it != packed_blocks_.end()) {
            it->second.lengths[slot - 1] = 10;
            it->second.live_count--;
        }
    }
    return moved;
}
