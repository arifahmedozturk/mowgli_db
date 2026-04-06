#include "catalog/table.h"
#include <algorithm>
#include <cstring>
#include <stdexcept>

Table::Table(Schema schema,
             std::unique_ptr<DiskManager> dm,
             std::unique_ptr<HeapFile> heap)
    : schema_(std::move(schema))
    , dm_(std::move(dm))
    , heap_(std::move(heap))
    , trie_(std::make_unique<DiskTrie>(*dm_)) {}

Table Table::create(const Schema& schema,
                    const std::string& trie_path,
                    const std::string& heap_path) {
    return Table(schema,
                 std::make_unique<DiskManager>(DiskManager::create(trie_path)),
                 std::make_unique<HeapFile>(HeapFile::create(heap_path)));
}

Table Table::open(const Schema& schema,
                  const std::string& trie_path,
                  const std::string& heap_path) {
    return Table(schema,
                 std::make_unique<DiskManager>(DiskManager::open(trie_path)),
                 std::make_unique<HeapFile>(HeapFile::open(heap_path)));
}

std::vector<uint8_t> Table::serialize(const Row& row) const {
    if (row.size() != schema_.cols.size())
        throw std::runtime_error("row column count mismatch");

    std::vector<uint8_t> buf;
    for (size_t i = 0; i < schema_.cols.size(); i++) {
        const ColDef& col  = schema_.cols[i];
        const auto&   data = row[i];

        if (col.type == ColType::UINT64) {
            if (data.size() != 8)
                throw std::runtime_error("UINT64 column must be 8 bytes");
            buf.insert(buf.end(), data.begin(), data.end());
        } else {
            if (data.size() > col.max_size)
                throw std::runtime_error("VARCHAR value exceeds max_size");
            uint16_t len = static_cast<uint16_t>(data.size());
            buf.push_back(len & 0xFF);
            buf.push_back((len >> 8) & 0xFF);
            buf.insert(buf.end(), data.begin(), data.end());
            buf.resize(buf.size() + (col.max_size - data.size()), 0);
        }
    }
    return buf;
}

Row Table::deserialize(const std::vector<uint8_t>& buf) const {
    Row row;
    size_t offset = 0;
    for (const ColDef& col : schema_.cols) {
        if (col.type == ColType::UINT64) {
            row.push_back(std::vector<uint8_t>(buf.begin() + offset,
                                               buf.begin() + offset + 8));
            offset += 8;
        } else {
            uint16_t len = static_cast<uint16_t>(buf[offset]) |
                           (static_cast<uint16_t>(buf[offset + 1]) << 8);
            offset += 2;
            row.push_back(std::vector<uint8_t>(buf.begin() + offset,
                                               buf.begin() + offset + len));
            offset += col.max_size;
        }
    }
    return row;
}

void Table::compact() {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    trie_->compact();
}

bool Table::insert(const Row& row) {
    const auto& pk_data = row[schema_.pk_col];
    std::string pk_key(pk_data.begin(), pk_data.end());

    if (trie_->lookup(pk_key)) return false;

    auto buf      = serialize(row);
    RecordPtr ptr = heap_->insert(buf.data(), static_cast<uint16_t>(buf.size()));
    trie_->insert(pk_key, ptr);
    dm_->set_key_count(dm_->key_count() + 1);
    return true;
}

size_t Table::bulk_insert(std::vector<Row> rows) {
    std::sort(rows.begin(), rows.end(),
              [this](const Row& a, const Row& b) {
                  return a[schema_.pk_col] < b[schema_.pk_col];
              });

    std::vector<std::pair<std::string, RecordPtr>> kvs;
    kvs.reserve(rows.size());
    size_t inserted = 0;
    for (const auto& row : rows) {
        const auto& pk_data = row[schema_.pk_col];
        std::string pk_key(pk_data.begin(), pk_data.end());
        if (trie_->lookup(pk_key)) continue;
        auto buf      = serialize(row);
        RecordPtr ptr = heap_->insert(buf.data(), static_cast<uint16_t>(buf.size()));
        kvs.push_back({std::move(pk_key), ptr});
        inserted++;
    }

    trie_->bulk_insert(std::move(kvs));
    dm_->set_key_count(dm_->key_count() + inserted);
    return inserted;
}

bool Table::update(const Row& row) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    const auto& pk_data = row[schema_.pk_col];
    std::string pk_key(pk_data.begin(), pk_data.end());

    RecordPtr ptr;
    if (!trie_->lookup(pk_key, &ptr)) return false;

    auto buf = serialize(row);
    return heap_->update(ptr, buf.data(), static_cast<uint16_t>(buf.size()));
}

bool Table::lookup(const std::vector<uint8_t>& pk, Row* row_out, size_t* chains_out) {
    std::string pk_key(pk.begin(), pk.end());
    RecordPtr ptr;
    if (!trie_->lookup(pk_key, &ptr, chains_out)) return false;
    if (row_out) {
        std::vector<uint8_t> buf;
        if (!heap_->read(ptr, buf)) return false;
        *row_out = deserialize(buf);
    }
    return true;
}

std::vector<Row> Table::range(const std::vector<uint8_t>& lo,
                              const std::vector<uint8_t>& hi) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    std::string lo_key(lo.begin(), lo.end());
    std::string hi_key(hi.begin(), hi.end());

    std::vector<std::pair<std::string, RecordPtr>> matches;
    trie_->range_scan(lo_key, hi_key, matches);

    std::vector<Row> rows;
    rows.reserve(matches.size());
    for (auto& [key, ptr] : matches) {
        std::vector<uint8_t> buf;
        if (heap_->read(ptr, buf))
            rows.push_back(deserialize(buf));
    }
    return rows;
}

std::vector<Row> Table::in_lookup(const std::vector<std::vector<uint8_t>>& pks) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    std::vector<Row> rows;
    for (const auto& pk : pks) {
        std::string pk_key(pk.begin(), pk.end());
        RecordPtr ptr;
        if (!trie_->lookup(pk_key, &ptr)) continue;
        std::vector<uint8_t> buf;
        if (heap_->read(ptr, buf))
            rows.push_back(deserialize(buf));
    }
    return rows;
}

bool Table::remove(const std::vector<uint8_t>& pk) {
    std::string pk_key(pk.begin(), pk.end());
    RecordPtr ptr;
    if (!trie_->lookup(pk_key, &ptr)) return false;
    heap_->remove(ptr);
    trie_->remove(pk_key);
    dm_->set_key_count(dm_->key_count() - 1);
    return true;
}
