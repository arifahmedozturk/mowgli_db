#include "catalog/table.h"
#include "catalog/codec.h"
#include <algorithm>
#include <cstdio>
#include <shared_mutex>
#include <stdexcept>

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
                             const std::function<bool(Row&&)>& fn) {
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
    bool go = true;
    ix->trie->range_visit(lo_key, hi_key, [&](const std::string& key, RecordPtr) {
        std::vector<std::vector<uint8_t>> parts = decode_key(types, key);
        RecordPtr ptr;
        if (trie_->lookup(std::string(parts.back().begin(), parts.back().end()), &ptr))
            heap_->with_row(ptr, [&](const uint8_t* r, uint16_t n) { go = fn(deserialize(r, n)); });
        return go;
    });
}

uint64_t Table::index_entry_count(const std::string& index) const {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    const SecondaryIndex* ix = find_index(index);
    if (!ix) throw std::runtime_error("no index '" + index + "'");
    return ix->dm->key_count();
}
