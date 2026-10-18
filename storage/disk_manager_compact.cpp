#include "storage/disk_manager.h"
#include <cassert>
#include <mutex>
#include <unordered_map>

// The compaction slide: live blocks move down to the front of the file.

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
