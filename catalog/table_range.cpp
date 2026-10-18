#include "catalog/table.h"
#include <shared_mutex>
#include <string>
#include <vector>

// Range scans over the primary key.

std::vector<Row> Table::range(const std::vector<uint8_t>& lo,
                              const std::vector<uint8_t>& hi) {
    std::vector<Row> rows;
    range_each(lo, hi, [&](Row&& r) { rows.push_back(std::move(r)); });
    return rows;
}

void Table::range_each(const std::vector<uint8_t>& lo, const std::vector<uint8_t>& hi,
                       const std::function<void(Row&&)>& fn) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    std::string lo_key(lo.begin(), lo.end());
    std::string hi_key(hi.begin(), hi.end());
    // The trie hands over the keys' record pointers a batch at a time; the
    // batch's heap pages are prefetched (#26), then each row is decoded
    // straight from its page.
    constexpr size_t BATCH = 4096;   // a typical scan's pages are all requested at once
    std::vector<RecordPtr> batch;
    auto drain = [&] {
        heap_->prefetch(batch);
        for (RecordPtr ptr : batch)
            heap_->with_row(ptr, [&](const uint8_t* row, uint16_t size) { fn(deserialize(row, size)); });
        batch.clear();
    };
    trie_->range_visit(lo_key, hi_key, [&](const std::string&, RecordPtr ptr) {
        batch.push_back(ptr);
        if (batch.size() == BATCH) drain();
        return true;
    });
    drain();
}
