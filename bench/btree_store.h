#pragma once
// The embedded B-trees bench_btree measures (#105): one interface, with an
// LMDB store (btree_lmdb.cpp) and a SQLite store (btree_sqlite.cpp), each
// built only when its library is found.
#include "bench/keys.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// One embedded store under test. open() creates or reopens the file at path.
struct Store {
    virtual ~Store() = default;
    virtual void     open(const std::string& path) = 0;
    virtual void     close() = 0;
    virtual void     bulk_load(const std::vector<std::pair<Key, std::string>>& sorted) = 0;
    virtual bool     insert(const Key& k, const std::string& v) = 0;
    virtual bool     lookup(const Key& k, std::string& v) = 0;
    virtual size_t   range(const Key& lo, const Key& hi) = 0;  // rows copied out
    virtual uint64_t used_bytes() = 0;
};

// a <= b, comparing bytes (for range scans' upper bound).
inline bool key_le(const uint8_t* a, size_t alen, const Key& b) {
    int c = std::memcmp(a, b.data(), std::min(alen, b.size()));
    return c < 0 || (c == 0 && alen <= b.size());
}

#ifdef HAVE_LMDB
std::unique_ptr<Store> make_lmdb_store();
#endif
#ifdef HAVE_SQLITE
std::unique_ptr<Store> make_sqlite_store();
#endif
