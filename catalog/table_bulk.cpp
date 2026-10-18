#include "catalog/table.h"
#include <algorithm>
#include <stdexcept>
#include <cstdint>
#include <cstring>
#include <shared_mutex>
#include <string>
#include <vector>

// Loading many rows at once.

// The order to load rows in: by primary key, and of several rows with the
// same key the first one given first (it wins, as with repeated single
// inserts). Sorts (first 8 key bytes, index) pairs rather than the rows
// themselves: moving a million Rows in a stable sort took 0.8 s of a 2.1 s
// load (#69).
static std::vector<uint32_t> load_order(const std::vector<Row>& rows, size_t pk_col) {
    // Each item carries its key's bytes, so comparing two keys that agree on
    // their head reads them directly rather than through the rows.
    struct Item { uint64_t head; const uint8_t* key; uint32_t len; uint32_t i; };
    std::vector<Item> items(rows.size());
    for (size_t i = 0; i < rows.size(); i++) {
        const auto& k = rows[i][pk_col];
        uint64_t head = 0;   // big-endian, so it orders like the bytes
        for (size_t j = 0; j < 8; j++) head = head << 8 | (j < k.size() ? k[j] : 0);
        items[i] = {head, k.data(), static_cast<uint32_t>(k.size()), static_cast<uint32_t>(i)};
    }
    std::sort(items.begin(), items.end(), [](const Item& x, const Item& y) {
        if (x.head != y.head) return x.head < y.head;
        int c = memcmp(x.key, y.key, std::min(x.len, y.len));
        if (c != 0) return c < 0;
        if (x.len != y.len) return x.len < y.len;
        return x.i < y.i;
    });
    std::vector<uint32_t> order(items.size());
    for (size_t i = 0; i < items.size(); i++) order[i] = items[i].i;
    return order;
}

size_t Table::bulk_insert(std::vector<Row> rows) {
    if (rows.size() > UINT32_MAX) throw std::runtime_error("bulk_insert: too many rows at once");
    const std::vector<uint32_t> order = load_order(rows, schema_.pk_col);

    std::unique_lock<std::shared_mutex> lock(table_latch_);

    std::vector<std::pair<std::string, RecordPtr>> kvs;
    kvs.reserve(rows.size());
    std::vector<const Row*> added;   // for the secondary indexes
    size_t inserted = 0;
    for (uint32_t i : order) {
        const Row& row = rows[i];
        const auto& pk_data = row[schema_.pk_col];
        std::string pk_key(pk_data.begin(), pk_data.end());
        if (!kvs.empty() && kvs.back().first == pk_key) continue;   // duplicate within the batch
        if (trie_->lookup(pk_key)) continue;
        auto buf      = serialize(row);
        RecordPtr ptr = heap_->insert(buf.data(), static_cast<uint16_t>(buf.size()));
        kvs.push_back({std::move(pk_key), ptr});
        if (!indexes_.empty()) added.push_back(&row);
        inserted++;
    }

    trie_->bulk_insert(std::move(kvs));
    dm_->set_key_count(dm_->key_count() + inserted);
    for (auto& ix : indexes_) {
        std::vector<std::pair<std::string, RecordPtr>> entries;
        entries.reserve(added.size());
        for (const Row* r : added) entries.push_back({index_key(ix, *r), RecordPtr{0, 0}});
        ix.trie->bulk_insert(std::move(entries));
        ix.dm->set_key_count(ix.dm->key_count() + added.size());
    }
    return inserted;
}
