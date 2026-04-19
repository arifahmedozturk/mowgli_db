#pragma once
#include "index/disk_trie.h"
#include "storage/disk_manager.h"
#include "storage/heap.h"
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

// A column type. Only fixed-width for now; varchar uses max_size bytes on disk.
enum class ColType { UINT64, VARCHAR };

struct ColDef {
    std::string name;
    ColType     type;
    uint16_t    max_size; // bytes: 8 for UINT64, caller-specified for VARCHAR
};

struct Schema {
    std::string          table_name;
    std::vector<ColDef>  cols;
    size_t               pk_col; // index of the primary key column
};

// A row is a parallel vector of raw byte buffers, one per column.
using Row = std::vector<std::vector<uint8_t>>;

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
        : schema_(std::move(o.schema_))
        , dm_    (std::move(o.dm_))
        , heap_  (std::move(o.heap_))
        , trie_  (std::move(o.trie_))
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

    // IN lookup: returns rows for each key that exists (preserves input order).
    std::vector<Row> in_lookup(const std::vector<std::vector<uint8_t>>& pks);

    // Compact: repack all chains on disk for better read locality.
    void compact();
    void compact_lex();

    const Schema& schema() const { return schema_; }
    uint64_t chain_count() const { return dm_->chain_count(); }
    size_t   active_chain_count() const { return trie_->active_chain_count(); }
    uint64_t record_count() const { return dm_->key_count(); }

private:
    Table(Schema schema, std::unique_ptr<DiskManager> dm, std::unique_ptr<HeapFile> heap);

    Schema                       schema_;
    std::unique_ptr<DiskManager> dm_;
    std::unique_ptr<HeapFile>    heap_;
    std::unique_ptr<DiskTrie>    trie_;
    mutable std::shared_mutex    table_latch_; // shared=lookup, exclusive=insert/update/remove

    // Serialize a row into a flat byte buffer for heap storage.
    std::vector<uint8_t> serialize(const Row& row) const;
    // Deserialize a flat byte buffer back into a Row.
    Row deserialize(const std::vector<uint8_t>& buf) const;
};
