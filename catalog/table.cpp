#include "catalog/table.h"
#include <algorithm>
#include <cstring>
#include <stdexcept>

Table::Table(Schema schema,
             std::unique_ptr<DiskManager> dm,
             std::unique_ptr<HeapFile> heap,
             const std::string& trie_path)
    : schema_(std::move(schema))
    , dm_(std::move(dm))
    , heap_(std::move(heap))
    , trie_(std::make_unique<DiskTrie>(*dm_, trie_path + ".bloom")) {}

Table Table::create(const Schema& schema,
                    const std::string& trie_path,
                    const std::string& heap_path) {
    return Table(schema,
                 DiskManager::create(trie_path),
                 std::make_unique<HeapFile>(HeapFile::create(heap_path)),
                 trie_path);
}

Table Table::open(const Schema& schema,
                  const std::string& trie_path,
                  const std::string& heap_path) {
    return Table(schema,
                 DiskManager::open(trie_path),
                 std::make_unique<HeapFile>(HeapFile::open(heap_path)),
                 trie_path);
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

std::string Table::pk_of(const uint8_t* row, size_t size) const {
    size_t offset = 0;
    for (size_t i = 0; i < schema_.cols.size(); i++) {
        const ColDef& col = schema_.cols[i];
        if (col.type == ColType::UINT64) {
            if (i == schema_.pk_col) return std::string(reinterpret_cast<const char*>(row + offset), 8);
            offset += 8;
        } else {
            uint16_t len = static_cast<uint16_t>(row[offset] | (row[offset + 1] << 8));
            if (i == schema_.pk_col) return std::string(reinterpret_cast<const char*>(row + offset + 2), len);
            offset += 2 + col.max_size;
        }
        if (offset > size) break;
    }
    throw std::runtime_error("pk_of: malformed row");
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

void Table::compact_lex() {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    trie_->compact_lex();
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
            return place_in_full_page(b, pk_key, buf, moved);
        return heap_->insert_near(b, a, buf.data(), size);
    });
    if (!inserted) return false;
    // Repoint the keys whose rows the split moved.
    for (const auto& [bytes, ptr] : moved)
        trie_->set_record(pk_of(bytes.data(), bytes.size()), ptr);
    dm_->set_key_count(dm_->key_count() + 1);
    return true;
}

size_t Table::bulk_insert(std::vector<Row> rows) {
    std::sort(rows.begin(), rows.end(),
              [this](const Row& a, const Row& b) {
                  return a[schema_.pk_col] < b[schema_.pk_col];
              });

    std::unique_lock<std::shared_mutex> lock(table_latch_);

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
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    std::string pk_key(pk.begin(), pk.end());
    RecordPtr ptr;
    if (!trie_->lookup(pk_key, &ptr)) return false;
    heap_->remove(ptr);
    trie_->remove(pk_key);
    dm_->set_key_count(dm_->key_count() - 1);

    // A page left sparse absorbs its next page's rows when they fit (#67).
    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>> moved;
    if (heap_->merge_next(ptr.block_id, moved))
        for (const auto& [bytes, p] : moved) trie_->set_record(pk_of(bytes.data(), bytes.size()), p);
    return true;
}

RecordPtr Table::place_in_full_page(uint64_t page, const std::string& key, const std::vector<uint8_t>& row,
                                    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    const auto size = static_cast<uint16_t>(row.size());
    using KeyBytes = std::vector<std::pair<std::string, size_t>>;   // (key, bytes incl. slot entry), sorted
    auto keyed = [&](uint64_t b) {
        KeyBytes rows;
        for (const auto& [bytes, ptr] : heap_->page_rows(b))
            rows.push_back({pk_of(bytes.data(), bytes.size()), bytes.size() + 4});
        std::sort(rows.begin(), rows.end());
        return rows;
    };
    auto move_from = [&](uint64_t from, uint64_t to, auto pred) {
        heap_->move_rows(from, to, [&](const uint8_t* r, uint16_t n) { return pred(pk_of(r, n)); }, moved);
    };
    KeyBytes rows = keyed(page);
    uint64_t next = heap_->next_page(page);

    if (next != NULL_BLOCK && heap_->free_bytes(next) >= BLOCK_SIZE / 8) {
        // 1. The next page has real room: move this page's highest keys into it
        //    until the two pages' free space is about even.
        size_t budget = heap_->free_bytes(next) / 2, moving = 0, i = rows.size();
        while (i > 1 && moving + rows[i - 1].second <= budget) moving += rows[--i].second;
        if (i < rows.size()) {
            const std::string first_moved = rows[i].first;
            move_from(page, next, [&](const std::string& k) { return k >= first_moved; });
            return heap_->insert_near(key >= first_moved ? next : page, NULL_BLOCK, row.data(), size);
        }
    }

    if (next != NULL_BLOCK) {
        // 2. Both full: split two pages into three (B*-tree), each about two
        //    thirds full: a new page between them takes this page's top third
        //    and the next page's bottom third.
        KeyBytes nrows = keyed(next);
        KeyBytes all = rows;
        all.insert(all.end(), nrows.begin(), nrows.end());
        size_t total = 0;
        for (const auto& r : all) total += r.second;
        size_t acc = 0, c1 = all.size(), c2 = all.size();
        for (size_t i = 0; i < all.size(); i++) {
            acc += all[i].second;
            if (c1 == all.size() && acc > total / 3)     c1 = i;
            if (c2 == all.size() && acc > total * 2 / 3) c2 = i;
        }
        if (c1 > 0 && c2 > c1 && c2 < all.size()) {
            const std::string lo = all[c1].first, hi = all[c2].first;   // middle page: keys in [lo, hi)
            uint64_t mid = heap_->new_page_after(page);
            move_from(page, mid, [&](const std::string& k) { return k >= lo; });
            move_from(next, mid, [&](const std::string& k) { return k < hi; });
            uint64_t target = key < lo ? page : key < hi ? mid : next;
            return heap_->insert_near(target, NULL_BLOCK, row.data(), size);
        }
    }

    // 3. Split at the median key: both halves about half full; the row joins its side.
    const std::string median = rows[rows.size() / 2].first;
    uint64_t upper = heap_->split_page(page, [&](const uint8_t* r, uint16_t n) { return pk_of(r, n) >= median; },
                                       moved);
    return heap_->insert_near(key < median ? page : upper, NULL_BLOCK, row.data(), size);
}
