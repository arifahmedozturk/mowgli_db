#include "storage/disk_manager.h"
#include <algorithm>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <unordered_set>
#include <fcntl.h>
#include <unistd.h>

static constexpr uint32_t FILE_MAGIC = 0x48545249; // "HTRI"

// Maximum free-block IDs stored inline in the header block after the fixed fields.
static constexpr size_t MAX_FREE_IN_HEADER =
    (BLOCK_SIZE - 32 /*sizeof FileHeader*/) / sizeof(uint64_t);

#pragma pack(push, 1)
struct FileHeader {
    uint32_t magic;
    uint32_t free_list_count;  // number of recycled IDs stored after this struct
    uint64_t root_block;
    uint64_t key_count;
    uint64_t next_free_block;
};
#pragma pack(pop)

static_assert(sizeof(FileHeader) == 32);

// ---- construction ----

DiskManager::DiskManager(int fd, uint64_t alloc_batch)
    : fd_(fd), alloc_batch_(alloc_batch) {}

DiskManager DiskManager::create(const std::string& path, uint64_t alloc_batch) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("cannot create file: " + path);
    DiskManager dm(fd, alloc_batch);
    dm.flush_header();
    return dm;
}

DiskManager DiskManager::open(const std::string& path, uint64_t alloc_batch) {
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) throw std::runtime_error("cannot open file: " + path);
    DiskManager dm(fd, alloc_batch);
    dm.read_header();
    dm.rebuild_packed_state();
    return dm;
}

DiskManager::~DiskManager() {
    if (fd_ >= 0) {
        flush_header();
        pool_.flush_all(fd_);
        ::close(fd_);
    }
}

void DiskManager::flush_header() {
    size_t free_count = std::min(free_list_mem_.size(), MAX_FREE_IN_HEADER);

    uint8_t* frame = pool_.pin_exclusive(0, fd_);
    FileHeader hdr{};
    hdr.magic           = FILE_MAGIC;
    hdr.free_list_count = static_cast<uint32_t>(free_count);
    hdr.root_block      = root_block_;
    hdr.key_count       = key_count_;
    hdr.next_free_block = committed_ceil_;
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

    std::lock_guard<std::mutex> lock(header_mutex_);
    root_block_      = hdr.root_block;
    key_count_       = hdr.key_count;
    next_free_block_ = hdr.next_free_block;
    committed_ceil_  = hdr.next_free_block;
    free_list_mem_   = std::move(free_ids);
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

        if (off == 0 || len == 0) continue;

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
    const auto& s2 = packed_blocks_[phys];
    size_t avail = PACK_DATA_SIZE - s2.data_end;
    if (s2.num_slots < PACK_MAX_SLOTS && avail >= 18u /* sizeof(DiskHeader) — minimum chain bytes */)
        pack_candidates_.push_back(phys);
}

uint64_t DiskManager::alloc_chain_slot(const ChainData& chain) {
    uint8_t tmp[BLOCK_SIZE];
    size_t enc = chain_encode_slice(chain, tmp, sizeof(tmp));
    if (enc == 0)
        throw std::runtime_error("chain_encode_slice: chain too large");

    // Large chains get their own block (unchanged behaviour).
    if (enc > PACK_THRESHOLD) {
        uint64_t id = alloc_block();
        uint8_t full[BLOCK_SIZE]{};
        memcpy(full, tmp, enc);
        write_block(id, full);
        return id; // slot bits = 0
    }

    // Each slot reserves PACK_THRESHOLD bytes so the chain can grow in-place
    // without promotion (as long as it stays within PACK_THRESHOLD bytes).
    static constexpr uint16_t SLOT_CAP = static_cast<uint16_t>(PACK_THRESHOLD);

    // --- find or allocate a packed block with enough room ---
    std::unique_lock<std::mutex> lp(pack_mutex_);

    uint64_t target = NULL_BLOCK;
    for (size_t i = pack_candidates_.size(); i-- > 0;) {
        uint64_t cand = pack_candidates_[i];
        auto it = packed_blocks_.find(cand);
        if (it == packed_blocks_.end()) {
            pack_candidates_.erase(pack_candidates_.begin() + static_cast<ptrdiff_t>(i));
            continue;
        }
        const PackedBlockState& s = it->second;
        if (s.num_slots >= PACK_MAX_SLOTS ||
            PACK_DATA_SIZE - s.data_end < SLOT_CAP) {
            pack_candidates_.erase(pack_candidates_.begin() + static_cast<ptrdiff_t>(i));
            continue;
        }
        target = cand;
        break;
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
        pack_candidates_.push_back(new_phys);
        target = new_phys;
    }

    PackedBlockState& st  = packed_blocks_[target];
    uint8_t  new_slot     = static_cast<uint8_t>(st.num_slots + 1); // 1-indexed
    uint16_t abs_offset   = static_cast<uint16_t>(PACK_DATA_OFFSET + st.data_end);

    // Directory stores SLOT_CAP (the reserved capacity) as the len field so that
    // in-place updates stay within bounds without changing the directory.
    // chain_decode_slice uses DiskHeader.node_count to stop reading early.
    {
        uint8_t* frame = pool_.pin_exclusive(target, fd_);
        size_t de = PACK_DIR_OFFSET + (new_slot - 1) * 4;
        memcpy(frame + de,     &abs_offset, 2);
        memcpy(frame + de + 2, &SLOT_CAP,   2);
        uint16_t ns = new_slot;
        memcpy(frame + 4, &ns, 2);
        // Write the encoded chain data; zero the remaining reserved bytes.
        memcpy(frame + abs_offset, tmp, enc);
        if (enc < SLOT_CAP)
            memset(frame + abs_offset + enc, 0, SLOT_CAP - enc);
        pool_.unpin_exclusive(target);
    }

    st.offsets[new_slot - 1] = abs_offset;
    st.lengths[new_slot - 1] = SLOT_CAP;   // capacity, not current size
    st.num_slots              = new_slot;
    st.data_end               = static_cast<uint16_t>(st.data_end + SLOT_CAP);
    st.live_count++;

    // Remove from candidates if no longer useful.
    if (st.num_slots >= PACK_MAX_SLOTS ||
        PACK_DATA_SIZE - st.data_end < SLOT_CAP) {
        auto it = std::find(pack_candidates_.begin(), pack_candidates_.end(), target);
        if (it != pack_candidates_.end()) pack_candidates_.erase(it);
    }

    return make_chain_addr(target, new_slot);
}

void DiskManager::free_chain_slot(uint64_t chain_addr) {
    if (chain_addr == NULL_BLOCK) return;
    uint8_t  slot = chain_addr_slot(chain_addr);
    uint64_t phys = chain_addr_phys(chain_addr);

    if (slot == 0) {
        free_block(phys);
        return;
    }

    std::lock_guard<std::mutex> lp(pack_mutex_);
    auto it = packed_blocks_.find(phys);
    if (it == packed_blocks_.end()) return; // already gone

    PackedBlockState& st = it->second;
    uint16_t off = st.offsets[slot - 1];
    uint16_t len = st.lengths[slot - 1];
    if (len == 0) return; // already dead

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
        uint16_t zero = 0;
        size_t de = PACK_DIR_OFFSET + (slot - 1) * 4;
        memcpy(frame + de,     &zero, 2);
        memcpy(frame + de + 2, &zero, 2);
        pool_.unpin_exclusive(phys);
    }

    st.offsets[slot - 1] = 0;
    st.lengths[slot - 1] = 0;
    if (is_live) st.live_count--;

    // Free the dedicated block the chain was forwarded to.
    if (forward_addr != NULL_BLOCK)
        free_block(chain_addr_phys(forward_addr));

    // Release the physical packed block if every slot is dead.
    bool all_dead = true;
    for (uint8_t i = 0; i < st.num_slots; i++)
        if (st.lengths[i] > 0) { all_dead = false; break; }

    if (all_dead) {
        packed_blocks_.erase(it);
        auto ic = std::find(pack_candidates_.begin(), pack_candidates_.end(), phys);
        if (ic != pack_candidates_.end()) pack_candidates_.erase(ic);
        free_block(phys);
    }
}

ChainData DiskManager::read_chain_at(uint64_t chain_addr) const {
    uint8_t  slot = chain_addr_slot(chain_addr);
    uint64_t phys = chain_addr_phys(chain_addr);

    if (slot == 0) {
        // Dedicated block — original fast path.
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
        return read_chain_at(fwd); // one-level hop to dedicated block
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

    if (slot == 0) {
        // Dedicated block — existing behaviour.
        uint8_t buf[BLOCK_SIZE];
        if (!chain_encode(chain, buf))
            throw std::runtime_error("update_chain_at: chain too large");
        write_block(phys, buf);
        return chain_addr;
    }

    // --- Packed block ---

    // Snapshot current slot metadata (under pack lock).
    uint16_t old_off, old_len;
    {
        std::lock_guard<std::mutex> lp(pack_mutex_);
        auto it = packed_blocks_.find(phys);
        if (it == packed_blocks_.end())
            throw std::runtime_error("update_chain_at: unknown packed block");
        old_off = it->second.offsets[slot - 1];
        old_len = it->second.lengths[slot - 1];
    }

    // Check for an existing forwarding stub (chain was already promoted).
    {
        const uint8_t* frame = pool_.pin_shared(phys, fd_);
        uint16_t tag = 0;
        if (old_len >= 2) memcpy(&tag, frame + old_off, 2);
        if (tag == FORWARD_MAGIC && old_len >= 10) {
            uint64_t fwd;
            memcpy(&fwd, frame + old_off + 2, 8);
            pool_.unpin_shared(phys);
            // Write to the dedicated block the chain was forwarded to.
            uint8_t buf[BLOCK_SIZE];
            if (!chain_encode(chain, buf))
                throw std::runtime_error("update_chain_at: chain too large");
            write_block(chain_addr_phys(fwd), buf);
            return chain_addr; // logical address unchanged
        }
        pool_.unpin_shared(phys);
    }

    // Encode the new chain data.
    uint8_t tmp[BLOCK_SIZE];
    size_t enc = chain_encode_slice(chain, tmp, sizeof(tmp));
    if (enc == 0) throw std::runtime_error("update_chain_at: chain too large");

    if (enc <= old_len) {
        // Fits within the slot's reserved capacity — write in-place.
        // Do NOT change the directory len (it records capacity, not current size).
        uint8_t* frame = pool_.pin_exclusive(phys, fd_);
        memcpy(frame + old_off, tmp, enc);
        pool_.unpin_exclusive(phys);
        return chain_addr;
    }

    // Doesn't fit — promote to a dedicated block.
    uint64_t new_phys = alloc_block();
    {
        uint8_t full[BLOCK_SIZE]{};
        memcpy(full, tmp, enc);
        write_block(new_phys, full);
    }

    // Write forwarding stub to the old packed slot.
    {
        uint8_t* frame = pool_.pin_exclusive(phys, fd_);
        uint16_t fwd_tag = FORWARD_MAGIC;
        uint64_t fwd_addr = new_phys; // slot=0, dedicated
        uint16_t stub_len = 10;       // 2 (tag) + 8 (addr)
        size_t de = PACK_DIR_OFFSET + (slot - 1) * 4;
        memcpy(frame + de + 2, &stub_len, 2);
        memcpy(frame + old_off,     &fwd_tag,  2);
        memcpy(frame + old_off + 2, &fwd_addr, 8);
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

    return new_phys; // new dedicated addr (slot=0); caller uses it as a hint
}
