#include "storage/heap.h"
#include <algorithm>
#include <sys/mman.h>

void HeapFile::prefetch(std::vector<RecordPtr> ptrs) const {
    std::vector<uint64_t> pages;
    pages.reserve(ptrs.size());
    for (const RecordPtr& p : ptrs) pages.push_back(p.block_id);
    std::sort(pages.begin(), pages.end());
    pages.erase(std::unique(pages.begin(), pages.end()), pages.end());
    // One page gains nothing (its first touch faults anyway), and each run of
    // consecutive pages costs a system call even when the pages are already
    // cached, so warm scans would pay for every run: prefetch only when the
    // pages form at most MAX_RUNS runs, as in a bulk-loaded or compacted heap.
    constexpr size_t MAX_RUNS = 2;
    if (pages.size() < 2 || pages.back() >= next_free_block_) return;
    std::vector<std::pair<uint64_t, uint64_t>> runs;   // [first, last]
    for (uint64_t p : pages) {
        if (!runs.empty() && runs.back().second + 1 == p) runs.back().second = p;
        else if (runs.size() == MAX_RUNS) return;
        else runs.push_back({p, p});
    }
    ensure_mapped_const(pages.back());
    for (const auto& [first, last] : runs)
        ::madvise(const_cast<uint8_t*>(block_ptr(first)), (last - first + 1) * BLOCK_SIZE, MADV_WILLNEED);
}
