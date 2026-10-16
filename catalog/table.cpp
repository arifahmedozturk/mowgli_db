#include "catalog/table.h"
#include "catalog/codec.h"
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <stdexcept>

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
    return std::string(pk_view(row, size));
}

std::string_view Table::pk_view(const uint8_t* row, size_t size) const {
    size_t offset = 0;
    for (size_t i = 0; i < schema_.cols.size(); i++) {
        const ColDef& col = schema_.cols[i];
        if (col.type == ColType::UINT64) {
            if (i == schema_.pk_col) return std::string_view(reinterpret_cast<const char*>(row + offset), 8);
            offset += 8;
        } else {
            uint16_t len = static_cast<uint16_t>(row[offset] | (row[offset + 1] << 8));
            if (i == schema_.pk_col) return std::string_view(reinterpret_cast<const char*>(row + offset + 2), len);
            offset += 2 + col.max_size;
        }
        if (offset > size) break;
    }
    throw std::runtime_error("pk_of: malformed row");
}

Row Table::deserialize(const std::vector<uint8_t>& buf) const {
    return deserialize(buf.data(), buf.size());
}

Row Table::deserialize(const uint8_t* buf, size_t /*size*/) const {
    Row row;
    row.reserve(schema_.cols.size());
    size_t offset = 0;
    for (const ColDef& col : schema_.cols) {
        if (col.type == ColType::UINT64) {
            row.emplace_back(buf + offset, buf + offset + 8);
            offset += 8;
        } else {
            uint16_t len = static_cast<uint16_t>(buf[offset]) |
                           (static_cast<uint16_t>(buf[offset + 1]) << 8);
            offset += 2;
            row.emplace_back(buf + offset, buf + offset + len);
            offset += col.max_size;
        }
    }
    return row;
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

// ---- secondary indexes (#96) ----

std::string Table::index_path(const std::string& name) const {
    std::string base = trie_path_;
    const std::string ext = ".trie";
    if (base.size() > ext.size() && base.compare(base.size() - ext.size(), ext.size(), ext) == 0)
        base.resize(base.size() - ext.size());
    return base + "." + name + ".idx";
}

Table::SecondaryIndex Table::open_index(const IndexDef& def, bool create) const {
    SecondaryIndex ix;
    ix.def = def;
    for (size_t c : def.cols) {
        if (c >= schema_.cols.size()) throw std::runtime_error("index " + def.name + ": bad column");
        ix.key_types.push_back(schema_.cols[c].type);
    }
    ix.key_types.push_back(schema_.cols[schema_.pk_col].type);
    const std::string path = index_path(def.name);
    ix.dm   = create ? DiskManager::create(path) : DiskManager::open(path);
    ix.trie = std::make_unique<DiskTrie>(*ix.dm, path + ".bloom");
    return ix;
}

std::string Table::index_key(const SecondaryIndex& ix, const Row& row) const {
    std::string key;
    for (size_t i = 0; i < ix.def.cols.size(); i++)
        append_key_part(key, ix.key_types[i], row[ix.def.cols[i]], false);
    append_key_part(key, ix.key_types.back(), row[schema_.pk_col], true);
    return key;
}

void Table::index_add(const Row& row) {
    for (auto& ix : indexes_) {
        ix.trie->insert(index_key(ix, row), RecordPtr{0, 0});
        ix.dm->set_key_count(ix.dm->key_count() + 1);
    }
}

void Table::index_remove(const Row& row) {
    for (auto& ix : indexes_)
        if (ix.trie->remove(index_key(ix, row))) ix.dm->set_key_count(ix.dm->key_count() - 1);
}

const Table::SecondaryIndex* Table::find_index(const std::string& name) const {
    for (const auto& ix : indexes_)
        if (ix.def.name == name) return &ix;
    return nullptr;
}

void Table::create_index(const IndexDef& def) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    if (def.name.empty() || def.cols.empty()) throw std::runtime_error("index needs a name and columns");
    if (find_index(def.name)) throw std::runtime_error("index '" + def.name + "' already exists");
    SecondaryIndex ix = open_index(def, true);

    // Build over the existing rows, bottom-up from the sorted entries.
    std::vector<std::pair<std::string, RecordPtr>> entries;
    const ColDef& pk = schema_.cols[schema_.pk_col];
    const std::string all_keys(pk.type == ColType::UINT64 ? 9 : size_t(pk.max_size) + 1, '\xFF');
    trie_->range_visit(std::string(), all_keys, [&](const std::string&, RecordPtr ptr) {
        heap_->with_row(ptr, [&](const uint8_t* r, uint16_t n) {
            entries.push_back({index_key(ix, deserialize(r, n)), RecordPtr{0, 0}});
        });
        return true;
    });
    uint64_t count = entries.size();
    ix.trie->bulk_insert(std::move(entries));
    ix.dm->set_key_count(count);
    indexes_.push_back(std::move(ix));
    schema_.indexes.push_back(def);
}

void Table::drop_index(const std::string& name) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    auto it = std::find_if(indexes_.begin(), indexes_.end(),
                           [&](const SecondaryIndex& ix) { return ix.def.name == name; });
    if (it == indexes_.end()) throw std::runtime_error("no index '" + name + "'");
    const std::string path = index_path(name);
    indexes_.erase(it);   // closes the trie and its file
    std::remove(path.c_str());
    std::remove((path + ".bloom").c_str());
    schema_.indexes.erase(std::remove_if(schema_.indexes.begin(), schema_.indexes.end(),
                                         [&](const IndexDef& d) { return d.name == name; }),
                          schema_.indexes.end());
}

void Table::index_range_each(const std::string& index, const Row& lo, const Row& hi,
                             const std::function<void(Row&&)>& fn) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    const SecondaryIndex* ix = find_index(index);
    if (!ix) throw std::runtime_error("no index '" + index + "'");
    if (lo.size() != hi.size() || lo.empty() || lo.size() > ix->def.cols.size())
        throw std::runtime_error("index bounds must cover a prefix of the index's columns");

    std::string lo_key, hi_key;
    for (size_t i = 0; i < lo.size(); i++) {
        append_key_part(lo_key, ix->key_types[i], lo[i], false);
        append_key_part(hi_key, ix->key_types[i], hi[i], false);
    }
    // Above every continuation of hi: longer than the longest encoding of the
    // remaining parts (escaped strings, numbers, the raw primary key).
    size_t rest = 1;
    for (size_t i = lo.size(); i < ix->key_types.size(); i++) {
        size_t col = i < ix->def.cols.size() ? ix->def.cols[i] : schema_.pk_col;
        rest += ix->key_types[i] == ColType::UINT64 ? 8 : 2 * size_t(schema_.cols[col].max_size) + 2;
    }
    hi_key.append(rest, '\xFF');

    const std::vector<ColType>& types = ix->key_types;
    ix->trie->range_visit(lo_key, hi_key, [&](const std::string& key, RecordPtr) {
        std::vector<std::vector<uint8_t>> parts = decode_key(types, key);
        RecordPtr ptr;
        if (trie_->lookup(std::string(parts.back().begin(), parts.back().end()), &ptr))
            heap_->with_row(ptr, [&](const uint8_t* r, uint16_t n) { fn(deserialize(r, n)); });
        return true;
    });
}

uint64_t Table::index_entry_count(const std::string& index) const {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    const SecondaryIndex* ix = find_index(index);
    if (!ix) throw std::runtime_error("no index '" + index + "'");
    return ix->dm->key_count();
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
    if (!moved.empty()) trie_->set_records(moved_keys(moved));
    dm_->set_key_count(dm_->key_count() + 1);
    if (!indexes_.empty()) index_add(row);
    return true;
}

size_t Table::bulk_insert(std::vector<Row> rows) {
    // Stable: of several rows with the same key, the first one given wins,
    // as with repeated single inserts.
    std::stable_sort(rows.begin(), rows.end(),
              [this](const Row& a, const Row& b) {
                  return a[schema_.pk_col] < b[schema_.pk_col];
              });

    std::unique_lock<std::shared_mutex> lock(table_latch_);

    std::vector<std::pair<std::string, RecordPtr>> kvs;
    kvs.reserve(rows.size());
    std::vector<const Row*> added;   // for the secondary indexes
    size_t inserted = 0;
    for (const auto& row : rows) {
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
    // Each row is decoded straight from its heap page as the trie walks the keys.
    trie_->range_visit(lo_key, hi_key, [&](const std::string&, RecordPtr ptr) {
        heap_->with_row(ptr, [&](const uint8_t* row, uint16_t size) { fn(deserialize(row, size)); });
        return true;
    });
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

RecordPtr Table::place_in_full_page(uint64_t page, const std::string& key, const std::vector<uint8_t>& row,
                                    std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) {
    const auto size = static_cast<uint16_t>(row.size());
    // (key, bytes incl. slot entry), sorted. Keys are views into the pages, so
    // they are only read before any rows move (#68).
    using KeyBytes = std::vector<std::pair<std::string_view, size_t>>;
    auto keyed = [&](uint64_t b) {
        // Sort on the first 8 key bytes as a big-endian integer (the same order
        // as comparing the bytes), then the whole key on ties.
        struct Item { uint64_t head; std::string_view key; size_t bytes; };
        std::vector<Item> items;
        heap_->for_each_row(b, [&](const uint8_t* r, uint16_t n) {
            std::string_view k = pk_view(r, n);
            uint64_t head = 0;
            for (size_t i = 0; i < 8; i++)
                head = head << 8 | (i < k.size() ? static_cast<uint8_t>(k[i]) : 0);
            items.push_back({head, k, size_t(n) + 4});
        });
        std::sort(items.begin(), items.end(), [](const Item& x, const Item& y) {
            return x.head != y.head ? x.head < y.head : x.key < y.key;
        });
        KeyBytes rows;
        rows.reserve(items.size());
        for (const auto& it : items) rows.push_back({it.key, it.bytes});
        return rows;
    };
    auto move_from = [&](uint64_t from, uint64_t to, auto pred) {
        heap_->move_rows(from, to, [&](const uint8_t* r, uint16_t n) { return pred(pk_view(r, n)); }, moved);
    };
    KeyBytes rows = keyed(page);
    uint64_t next = heap_->next_page(page);

    if (next != NULL_BLOCK && heap_->free_bytes(next) >= BLOCK_SIZE / 8) {
        // 1. The next page has real room: move this page's highest keys into it
        //    until the two pages' free space is about even.
        size_t budget = heap_->free_bytes(next) / 2, moving = 0, i = rows.size();
        while (i > 1 && moving + rows[i - 1].second <= budget) moving += rows[--i].second;
        if (i < rows.size()) {
            const std::string first_moved(rows[i].first);
            move_from(page, next, [&](std::string_view k) { return k >= first_moved; });
            return heap_->insert_near(key >= first_moved ? next : page, NULL_BLOCK, row.data(), size);
        }
    }

    if (next != NULL_BLOCK) {
        // 2. Both full: split two pages into three (B*-tree), each about two
        //    thirds full: a new page between them takes this page's top third
        //    and the next page's bottom third. The next page's keys all follow
        //    this page's, so the two sorted lists read as one.
        KeyBytes nrows = keyed(next);
        auto at = [&](size_t i) -> const std::pair<std::string_view, size_t>& {
            return i < rows.size() ? rows[i] : nrows[i - rows.size()];
        };
        const size_t n = rows.size() + nrows.size();
        size_t total = 0;
        for (size_t i = 0; i < n; i++) total += at(i).second;
        size_t acc = 0, c1 = n, c2 = n;
        for (size_t i = 0; i < n; i++) {
            acc += at(i).second;
            if (c1 == n && acc > total / 3)     c1 = i;
            if (c2 == n && acc > total * 2 / 3) c2 = i;
        }
        if (c1 > 0 && c2 > c1 && c2 < n) {
            const std::string lo(at(c1).first), hi(at(c2).first);   // middle page: keys in [lo, hi)
            uint64_t mid = heap_->new_page_after(page);
            move_from(page, mid, [&](std::string_view k) { return k >= lo; });
            move_from(next, mid, [&](std::string_view k) { return k < hi; });
            uint64_t target = key < lo ? page : key < hi ? mid : next;
            return heap_->insert_near(target, NULL_BLOCK, row.data(), size);
        }
    }

    // 3. Split at the median key: both halves about half full; the row joins its side.
    const std::string median(rows[rows.size() / 2].first);
    uint64_t upper = heap_->split_page(page, [&](const uint8_t* r, uint16_t n) { return pk_view(r, n) >= median; },
                                       moved);
    return heap_->insert_near(key < median ? page : upper, NULL_BLOCK, row.data(), size);
}
