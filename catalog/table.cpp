#include "catalog/table.h"
#include <shared_mutex>
#include <stdexcept>

// Opening a table, compaction, and single-row writes and reads. The row
// format is in table_row.cpp, secondary indexes in table_index.cpp, range
// scans in table_range.cpp, bulk loads in table_bulk.cpp and commit in
// table_commit.cpp.

Table::Table(Schema schema,
             std::unique_ptr<DiskManager> dm,
             std::unique_ptr<HeapFile> heap,
             const std::string& trie_path,
             bool create)
    : schema_(std::move(schema))
    , trie_path_(trie_path)
    , dm_(std::move(dm))
    , heap_(std::move(heap))
    , trie_(std::make_unique<DiskTrie>(*dm_, trie_path + ".bloom")) {
    for (const auto& def : schema_.indexes) indexes_.push_back(open_index(def, create));
}

Table Table::create(const Schema& schema,
                    const std::string& trie_path,
                    const std::string& heap_path) {
    return Table(schema,
                 DiskManager::create(trie_path),
                 std::make_unique<HeapFile>(HeapFile::create(heap_path)),
                 trie_path, true);
}

Table Table::open(const Schema& schema,
                  const std::string& trie_path,
                  const std::string& heap_path) {
    return Table(schema,
                 DiskManager::open(trie_path),
                 std::make_unique<HeapFile>(HeapFile::open(heap_path)),
                 trie_path, false);
}

void Table::compact() {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    trie_->compact();
    for (auto& ix : indexes_) ix.trie->compact();
}

void Table::compact_lex() {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    trie_->compact_lex();
    for (auto& ix : indexes_) ix.trie->compact_lex();
}

bool Table::insert(const Row& row) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);  // check-then-insert must be atomic
    const auto& pk_data = row[schema_.pk_col];
    std::string pk_key(pk_data.begin(), pk_data.end());

    // The trie places the key first and asks for the row only once it isn't a
    // duplicate. Heap pages hold contiguous key ranges (#65): the row goes on
    // a neighbour's page. A full page the key falls inside first shifts its
    // highest keys into the next page if that has room, else splits at its
    // median (#67), like B-tree leaves.
    auto buf      = serialize(row);
    auto size     = static_cast<uint16_t>(buf.size());
    bool inserted = false;
    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> moved;
    trie_->insert(pk_key, [&](RecordPtr before, RecordPtr after) {
        inserted = true;
        uint64_t b = before.valid() ? before.block_id : NULL_BLOCK;
        uint64_t a = after.valid()  ? after.block_id  : NULL_BLOCK;
        if (b != NULL_BLOCK && b == a && !heap_->has_room(b, size))
            return heap_->place_in_full_page(b, pk_key, buf.data(), size,
                                             [this](const uint8_t* r, uint16_t n) { return pk_view(r, n); }, moved);
        return heap_->insert_near(b, a, buf.data(), size);
    });
    if (!inserted) return false;
    // Repoint the keys whose rows the split moved.
    if (!moved.empty()) trie_->set_records(moved_keys(moved));
    dm_->set_key_count(dm_->key_count() + 1);
    if (!indexes_.empty()) index_add(row);
    return true;
}

bool Table::update(const Row& row) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    const auto& pk_data = row[schema_.pk_col];
    std::string pk_key(pk_data.begin(), pk_data.end());

    RecordPtr ptr;
    if (!trie_->lookup(pk_key, &ptr)) return false;

    Row old;
    if (!indexes_.empty()) {
        std::vector<uint8_t> bytes;
        if (!heap_->read(ptr, bytes)) return false;
        old = deserialize(bytes);
    }
    auto buf = serialize(row);
    if (!heap_->update(ptr, buf.data(), static_cast<uint16_t>(buf.size()))) return false;
    for (auto& ix : indexes_) {
        std::string before = index_key(ix, old), after = index_key(ix, row);
        if (before == after) continue;
        ix.trie->remove(before);
        ix.trie->insert(after, RecordPtr{0, 0});
    }
    return true;
}

bool Table::lookup(const std::vector<uint8_t>& pk, Row* row_out, size_t* chains_out) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
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

bool Table::lookup_view(const std::vector<uint8_t>& pk, const std::function<void(const RowView&)>& fn,
                        size_t* chains_out) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    RecordPtr ptr;
    if (!trie_->lookup(std::string(pk.begin(), pk.end()), &ptr, chains_out)) return false;
    return heap_->with_row(ptr, [&](const uint8_t* row, uint16_t) { fn(RowView(schema_, row)); });
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
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    std::string pk_key(pk.begin(), pk.end());
    RecordPtr ptr;
    if (!trie_->lookup(pk_key, &ptr)) return false;
    if (!indexes_.empty()) {
        std::vector<uint8_t> bytes;
        if (heap_->read(ptr, bytes)) index_remove(deserialize(bytes));
    }
    heap_->remove(ptr);
    trie_->remove(pk_key);
    dm_->set_key_count(dm_->key_count() - 1);

    // A page left sparse absorbs its next page's rows when they fit (#67).
    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> moved;
    if (heap_->merge_next(ptr.block_id, moved)) trie_->set_records(moved_keys(moved));
    return true;
}

std::vector<std::pair<std::string, RecordPtr>>
Table::moved_keys(const std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) const {
    std::vector<std::pair<std::string, RecordPtr>> kvs;
    kvs.reserve(moved.size());
    for (const auto& [bytes, ptr] : moved) kvs.push_back({pk_of(bytes.data(), bytes.size()), ptr});
    return kvs;
}
