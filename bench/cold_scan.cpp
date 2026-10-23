// Cold range scans (#26): evict a closed table's files from the page cache,
// reopen it, then time range scans that fault their index and heap pages in.
#include "bench/bench_common.h"
#include "bench/cold.h"
#include "bench/copy_out.h"
#include "bench/keys.h"
#include "bench/metrics.h"
#include "catalog/table.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>

void cold_range_run(const std::string& label, const Schema& s, const std::string& base,
                    const std::vector<std::pair<Key, Key>>& ranges, Metrics& m) {
    const std::string trie = base + ".trie", heap = base + ".heap";
    evict_from_page_cache(trie);
    evict_from_page_cache(heap);
    evict_from_page_cache(trie + ".bloom");
    Table t = Table::open(s, trie, heap);
    evict_from_page_cache(heap);   // opening reads the index, not the rows

    size_t rows = 0;
    long   flt0 = major_faults();
    auto   t0   = Clock::now();
    std::vector<std::string> cells;   // rows copied out as in the warm phases (#64)
    for (const auto& [lo, hi] : ranges)
        t.range_view(lo, hi, [&](const RowView& row) { copy_out(row, cells); rows++; });
    double secs   = Dur(Clock::now() - t0).count();
    double faults = ranges.empty() ? 0 : double(major_faults() - flt0) / ranges.size();

    std::cout << "COLD RANGE — " << label << " (" << ranges.size() << " scans after page-cache eviction)\n"
              << "  rows returned: " << rows << "\n"
              << "  throughput   : " << ops_per_sec(ranges.size(), secs) << " scans/sec\n"
              << "  major faults : " << faults << " per scan\n\n";
    m.add("cold_" + label + "_range",        ops_per_sec(ranges.size(), secs), "scans/s");
    m.add("cold_" + label + "_range_faults", faults,                           "faults");
}
