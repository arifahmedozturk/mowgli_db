#include "storage/disk_manager.h"
#include "storage/pack_slot.h"
#include <algorithm>
#include <cstring>
#include <mutex>
#include <stdexcept>

// Placing chains in packed slots or dedicated blocks, and freeing them.

uint64_t DiskManager::alloc_chain_slot(const ChainData& chain, uint64_t hint_phys) {
    uint8_t tmp[BLOCK_SIZE];
    size_t enc = chain_encode_slice(chain, tmp, sizeof(tmp));
    if (enc == 0)
        throw std::runtime_error("chain_encode_slice: chain too large");
    return place_bytes(tmp, enc, hint_phys);
}

void DiskManager::write_dedicated(uint64_t phys, const uint8_t* bytes, size_t enc) {
    uint8_t full[BLOCK_SIZE]{};
    memcpy(full, bytes, enc);
    write_block(phys, full);
}

uint64_t DiskManager::place_bytes(const uint8_t* bytes, size_t enc, uint64_t hint_phys) {
    // Large chains and buckets get their own block.
    if (enc > PACK_THRESHOLD) {
        uint64_t id = alloc_block();
        write_dedicated(id, bytes, enc);
        return id; // slot bits = 0
    }

    // Reserve the size class, not just enc bytes, so the chain can grow in place.
    const uint16_t cap = static_cast<uint16_t>(pack_slot_class(enc));

    std::unique_lock<std::mutex> lp(pack_mutex_);
    uint64_t target = find_pack_target(cap, hint_phys);
    if (target == NULL_BLOCK) target = new_packed_block(lp);
    PackedBlockState& st = packed_blocks_[target];

    // Reuse the best-fitting dead slot (its whole region), else append at the tail.
    int dead = st.dead_fit(cap);
    uint8_t slot = dead >= 0 ? fill_dead_slot(target, st, dead, bytes, enc)
                             : append_slot(target, st, cap, bytes, enc);
    return make_chain_addr(target, slot);
}

uint64_t DiskManager::find_pack_target(size_t cap, uint64_t hint_phys) {
    if (hint_phys != NULL_BLOCK && hint_phys != 0) {
        auto it = packed_blocks_.find(hint_phys);
        if (it != packed_blocks_.end() && it->second.fits(cap)) return hint_phys;
    }

    // Most recent candidates first. Blocks with no room for even the smallest
    // class are dropped; the scan is bounded so allocation stays cheap.
    size_t looked = 0;
    for (size_t i = pack_candidates_.size(); i-- > 0 && looked < 16;) {
        uint64_t cand = pack_candidates_[i];
        auto it = packed_blocks_.find(cand);
        if (it == packed_blocks_.end() || !it->second.fits(PACK_MIN_SLOT)) {
            if (it != packed_blocks_.end()) it->second.in_candidates = false;
            pack_candidates_.erase(pack_candidates_.begin() + static_cast<ptrdiff_t>(i));
            continue;
        }
        looked++;
        if (it->second.fits(cap)) return cand;
    }
    return NULL_BLOCK;
}

uint64_t DiskManager::new_packed_block(std::unique_lock<std::mutex>& lp) {
    // Release the pack lock while calling alloc_block.
    lp.unlock();
    uint64_t phys = alloc_block();
    lp.lock();
    // Write the packed block header (magic + num_slots=0, zeroed directory).
    uint8_t* frame = pool_.pin_exclusive(phys, fd_);
    memset(frame, 0, BLOCK_SIZE);
    uint32_t magic = PACK_BLOCK_MAGIC;
    memcpy(frame, &magic, 4);
    pool_.unpin_exclusive(phys);

    packed_blocks_[phys] = PackedBlockState{};
    packed_blocks_[phys].in_candidates = true;
    pack_candidates_.push_back(phys);
    return phys;
}

uint8_t DiskManager::fill_dead_slot(uint64_t phys, PackedBlockState& st, int dead,
                                    const uint8_t* bytes, size_t enc) {
    uint8_t  slot   = static_cast<uint8_t>(dead + 1);
    uint16_t off    = st.offsets[dead];
    uint16_t region = st.capacity(static_cast<size_t>(dead));
    uint8_t* frame  = pool_.pin_exclusive(phys, fd_);
    write_slot_entry(frame, slot, {off, region});
    memcpy(frame + off, bytes, enc);
    memset(frame + off + enc, 0, region - enc);
    pool_.unpin_exclusive(phys);
    st.lengths[dead] = region;
    return slot;
}

uint8_t DiskManager::append_slot(uint64_t phys, PackedBlockState& st, uint16_t cap,
                                 const uint8_t* bytes, size_t enc) {
    uint8_t  slot = static_cast<uint8_t>(st.num_slots + 1); // 1-indexed
    uint16_t off  = static_cast<uint16_t>(PACK_DATA_OFFSET + st.data_end);

    // The directory's len field records the slot's capacity (its size class),
    // so in-place updates stay within bounds; decoding stops at the encoded size.
    {
        uint8_t* frame = pool_.pin_exclusive(phys, fd_);
        write_slot_entry(frame, slot, {off, cap});
        uint16_t ns = slot;
        memcpy(frame + 4, &ns, 2);
        // Write the encoded chain data; zero the remaining reserved bytes.
        memcpy(frame + off, bytes, enc);
        if (enc < cap)
            memset(frame + off + enc, 0, cap - enc);
        pool_.unpin_exclusive(phys);
    }

    st.offsets[slot - 1] = off;
    st.lengths[slot - 1] = cap;   // capacity, not current size
    st.num_slots         = slot;
    st.data_end          = static_cast<uint16_t>(st.data_end + cap);

    // Remove from candidates if no longer useful.
    if (!st.fits(PACK_MIN_SLOT)) {
        auto it = std::find(pack_candidates_.begin(), pack_candidates_.end(), phys);
        if (it != pack_candidates_.end()) pack_candidates_.erase(it);
        st.in_candidates = false;
    }
    return slot;
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
    SlotEntry e{st.offsets[slot - 1], st.lengths[slot - 1]};
    if (e.len == 0) return NULL_BLOCK; // already dead

    // Read the slot under exclusive latch to check for a forwarding stub,
    // then immediately mark the directory entry dead.
    uint64_t forward_addr;
    {
        uint8_t* frame = pool_.pin_exclusive(phys, fd_);
        forward_addr = stub_target(frame, e);
        // Dead = length 0; the offset stays so the region can be reused.
        write_slot_entry(frame, slot, {e.off, 0});
        pool_.unpin_exclusive(phys);
    }

    st.lengths[slot - 1] = 0;

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
