#pragma once
#include "catalog/row_view.h"
#include "catalog/schema.h"
#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include "storage/heap.h"
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

class Table {
public:
    // Create new table files at trie_path / heap_path.
    static Table create(const Schema& schema,
                        const std::string& trie_path,
                        const std::string& heap_path);

    // Open existing table files.
    static Table open(const Schema& schema,
                      const std::string& trie_path,
                      const std::string& heap_path);

    Table(Table&& o) noexcept
        : schema_   (std::move(o.schema_))
        , trie_path_(std::move(o.trie_path_))
        , dm_       (std::move(o.dm_))
        , heap_     (std::move(o.heap_))
        , trie_     (std::move(o.trie_))
        , indexes_  (std::move(o.indexes_))
    {} // table_latch_ default-constructed (unlocked) — moved-from object is dead

    // Insert a new row. Returns false if the primary key already exists.
    bool insert(const Row& row);

    // Bulk insert: insert all rows, skipping duplicates, without per-insert
    // rebalancing. Rebuilds weights in one DFS pass at the end.
    // Much faster than calling insert() N times when loading large batches.
    size_t bulk_insert(std::vector<Row> rows);

    // Update an existing row by primary key. Returns false if not found.
    bool update(const Row& row);

    // Lookup by primary key. Returns false if not found.
    // chains_out receives the number of chain blocks traversed if non-null.
    bool lookup(const std::vector<uint8_t>& pk, Row* row_out = nullptr,
                size_t* chains_out = nullptr);

    // Delete by primary key. Returns false if not found.
    bool remove(const std::vector<uint8_t>& pk);

    // Range scan: returns all rows with lo <= pk <= hi, in lex order.
    std::vector<Row> range(const std::vector<uint8_t>& lo,
                           const std::vector<uint8_t>& hi);
    // The rows of [lo, hi] as views of their bytes in the heap, in key order:
    // no copy or allocation per row (#64). A view is valid only during fn,
    // which runs under the table's read latch and must not write to it.
    void range_view(const std::vector<uint8_t>& lo, const std::vector<uint8_t>& hi,
                    const std::function<void(const RowView&)>& fn);
    // The same rows handed to fn in key order, without collecting them (#75).
    // fn runs under the table's read latch, so it must not write to the table.
    void range_each(const std::vector<uint8_t>& lo, const std::vector<uint8_t>& hi,
                    const std::function<void(Row&&)>& fn);

    // IN lookup: returns rows for each key that exists (preserves input order).
    std::vector<Row> in_lookup(const std::vector<std::vector<uint8_t>>& pks);

    // ---- secondary indexes (#96) ----
    // Each index is a trie of (indexed cells..., primary key) with no row
    // pointer, kept up to date by every write; rows are fetched through the
    // primary key. Its file is <table>.<index>.idx next to the table's trie.
    // create_index builds it over the existing rows; both calls update
    // schema().indexes, which the caller persists.
    void create_index(const IndexDef& def);
    void drop_index(const std::string& name);
    // Rows whose indexed cells lie in [lo, hi], in index order. lo and hi hold
    // cells for a prefix of the index's columns (e.g. just the first), so
    // lo == hi is an equality match on that prefix. fn runs under the table's
    // read latch, so it must not write to the table.
    void index_range_each(const std::string& index, const Row& lo, const Row& hi,
                          const std::function<void(Row&&)>& fn);
    // Number of entries in an index (tests and diagnostics).
    uint64_t index_entry_count(const std::string& index) const;

    // Compact: repack all chains on disk for better read locality.
    void compact();
    void compact_lex();

    const Schema& schema() const { return schema_; }
    uint64_t chain_count() const { return dm_->chain_count(); }
    size_t   active_chain_count() const { return trie_->active_chain_count(); }
    uint64_t record_count() const { return dm_->key_count(); }
    // Bytes in allocated blocks (excludes the files' pre-grown tail).
    uint64_t index_bytes() const { return (chain_count() + 1) * BLOCK_SIZE; }
    uint64_t heap_bytes()  const { return (heap_->block_count() + 1) * BLOCK_SIZE; }
    uint64_t cache_hits()   const { return trie_->cache_hits(); }
    uint64_t cache_misses() const { return trie_->cache_misses(); }
    bool     bloom_loaded() const { return trie_->bloom_loaded(); }
    // Where a key's row lives in the heap (diagnostics and tests).
    bool     locate(const std::vector<uint8_t>& pk, RecordPtr* ptr) const {
        std::shared_lock<std::shared_mutex> lock(table_latch_);
        return trie_->lookup(std::string(pk.begin(), pk.end()), ptr);
    }

private:
    Table(Schema schema, std::unique_ptr<DiskManager> dm, std::unique_ptr<HeapFile> heap,
          const std::string& trie_path, bool create);

    struct SecondaryIndex {
        IndexDef                     def;
        std::vector<ColType>         key_types;   // the indexed columns, then the primary key
        std::unique_ptr<DiskManager> dm;
        std::unique_ptr<DiskTrie>    trie;        // refers to *dm: always released first

        SecondaryIndex() = default;
        SecondaryIndex(SecondaryIndex&&) = default;
        SecondaryIndex& operator=(SecondaryIndex&& o) noexcept {
            trie.reset();   // the trie flushes into its file, so it goes before dm
            dm.reset();
            def       = std::move(o.def);
            key_types = std::move(o.key_types);
            dm        = std::move(o.dm);
            trie      = std::move(o.trie);
            return *this;
        }
    };

    Schema                       schema_;
    std::string                  trie_path_;
    std::unique_ptr<DiskManager> dm_;
    std::unique_ptr<HeapFile>    heap_;
    std::unique_ptr<DiskTrie>    trie_;
    std::vector<SecondaryIndex>  indexes_;
    mutable std::shared_mutex    table_latch_; // shared=lookup, exclusive=insert/update/remove

    std::string     index_path(const std::string& name) const;
    SecondaryIndex  open_index(const IndexDef& def, bool create) const;
    // The entry key of row in ix: its indexed cells, then its primary key.
    std::string     index_key(const SecondaryIndex& ix, const Row& row) const;
    void            index_add(const Row& row);
    void            index_remove(const Row& row);
    const SecondaryIndex* find_index(const std::string& name) const;

    // Serialize a row into a flat byte buffer for heap storage.
    std::vector<uint8_t> serialize(const Row& row) const;
    // Deserialize a flat byte buffer back into a Row.
    Row deserialize(const std::vector<uint8_t>& buf) const;
    Row deserialize(const uint8_t* buf, size_t size) const;
    // The primary-key bytes of a serialized row.
    std::string pk_of(const uint8_t* row, size_t size) const;
    // The same bytes, viewed in place in the row.
    std::string_view pk_view(const uint8_t* row, size_t size) const;
    // (key, new record) for each moved row, for DiskTrie::set_records.
    std::vector<std::pair<std::string, RecordPtr>>
    moved_keys(const std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved) const;
    // Place a row whose key falls inside a full heap page (shift or split, #67);
    // rows that move are appended to moved for repointing.
    RecordPtr place_in_full_page(uint64_t page, const std::string& key, const std::vector<uint8_t>& row,
                                 std::vector<std::pair<std::vector<uint8_t>, RecordPtr>>& moved);
};
