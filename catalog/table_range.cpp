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
    range_view(lo, hi, [&](const RowView& v) { fn(v.to_row()); });
}

void Table::range_view(const std::vector<uint8_t>& lo, const std::vector<uint8_t>& hi,
                       const std::function<void(const RowView&)>& fn) {
    scan_rows(lo, hi, [&](const RowView& v) { fn(v); return true; });
}

void Table::scan_rows(const std::vector<uint8_t>& lo, const std::vector<uint8_t>& hi,
                      const std::function<bool(const RowView&)>& fn) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    std::string lo_key(lo.begin(), lo.end());
    std::string hi_key(hi.begin(), hi.end());
    // The trie hands over the keys' record pointers a batch at a time; the
    // batch's heap pages are prefetched (#26), then each row is viewed where
    // it lies in its page.
    constexpr size_t BATCH = 4096;   // a typical scan's pages are all requested at once
    std::vector<RecordPtr> batch;
    bool go = true;
    auto drain = [&] {
        heap_->prefetch(batch);
        for (RecordPtr ptr : batch) {
            if (!go) break;
            heap_->with_row(ptr, [&](const uint8_t* row, uint16_t) { go = fn(RowView(schema_, row)); });
        }
        batch.clear();
    };
    trie_->range_visit(lo_key, hi_key, [&](const std::string&, RecordPtr ptr) {
        batch.push_back(ptr);
        if (batch.size() == BATCH) drain();
        return go;
    });
    if (go) drain();
}
