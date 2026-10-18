#include "storage/disk_manager.h"
#include "storage/pack_slot.h"
#include <algorithm>
#include <cstring>
#include <mutex>
#include <unordered_set>

// Rebuilding the packed blocks' in-memory state when a file is opened.

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
        SlotEntry e = read_slot_entry(frame, static_cast<uint8_t>(i + 1));
        st.offsets[i] = e.off;
        st.lengths[i] = e.len;

        if (e.off == 0) continue;
        if (e.len == 0) {                     // dead: its region still occupies [off, next slot)
            uint16_t start = static_cast<uint16_t>(e.off - PACK_DATA_OFFSET);
            if (start > st.data_end) st.data_end = start;
            continue;
        }

        uint16_t slot_end = static_cast<uint16_t>(e.off + e.len - PACK_DATA_OFFSET);
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
