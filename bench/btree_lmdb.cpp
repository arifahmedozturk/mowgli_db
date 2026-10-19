// LMDB as a bench_btree store: MDB_WRITEMAP | MDB_NOSYNC | MDB_NOMETASYNC,
// reads through mmap, single inserts one transaction each.
#include "bench/btree_store.h"
#include <lmdb.h>
#include <stdexcept>

static void lmdb_check(int rc, const char* what) {
    if (rc != MDB_SUCCESS) throw std::runtime_error(std::string("lmdb ") + what + ": " + mdb_strerror(rc));
}

class LmdbStore : public Store {
public:
    ~LmdbStore() override { close(); }

    void open(const std::string& path) override {
        lmdb_check(mdb_env_create(&env_), "env_create");
        lmdb_check(mdb_env_set_mapsize(env_, 256ULL << 30), "set_mapsize");
        lmdb_check(mdb_env_open(env_, path.c_str(),
                                MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOSYNC | MDB_NOMETASYNC, 0644),
                   "env_open");
        MDB_txn* txn;
        lmdb_check(mdb_txn_begin(env_, nullptr, 0, &txn), "txn_begin");
        lmdb_check(mdb_dbi_open(txn, nullptr, 0, &dbi_), "dbi_open");
        lmdb_check(mdb_txn_commit(txn), "txn_commit");
        lmdb_check(mdb_txn_begin(env_, nullptr, MDB_RDONLY, &rtxn_), "read txn");
        mdb_txn_reset(rtxn_);
    }

    void close() override {
        if (rtxn_) { mdb_txn_abort(rtxn_); rtxn_ = nullptr; }
        if (env_)  { mdb_env_close(env_);  env_  = nullptr; }
    }

    void bulk_load(const std::vector<std::pair<Key, std::string>>& sorted) override {
        MDB_txn* txn;
        lmdb_check(mdb_txn_begin(env_, nullptr, 0, &txn), "txn_begin");
        for (const auto& [k, v] : sorted) {
            MDB_val mk = val(k), mv = val(v);
            int rc = mdb_put(txn, dbi_, &mk, &mv, MDB_APPEND);
            if (rc != MDB_KEYEXIST) lmdb_check(rc, "put");  // duplicate random keys
        }
        lmdb_check(mdb_txn_commit(txn), "txn_commit");
    }

    bool insert(const Key& k, const std::string& v) override {
        MDB_txn* txn;
        lmdb_check(mdb_txn_begin(env_, nullptr, 0, &txn), "txn_begin");
        MDB_val mk = val(k), mv = val(v);
        int rc = mdb_put(txn, dbi_, &mk, &mv, MDB_NOOVERWRITE);
        if (rc == MDB_KEYEXIST) { mdb_txn_abort(txn); return false; }
        lmdb_check(rc, "put");
        lmdb_check(mdb_txn_commit(txn), "txn_commit");
        return true;
    }

    bool lookup(const Key& k, std::string& v) override {
        lmdb_check(mdb_txn_renew(rtxn_), "txn_renew");
        MDB_val mk = val(k), mv;
        int rc = mdb_get(rtxn_, dbi_, &mk, &mv);
        if (rc == MDB_SUCCESS) v.assign(static_cast<const char*>(mv.mv_data), mv.mv_size);
        mdb_txn_reset(rtxn_);
        return rc == MDB_SUCCESS;
    }

    size_t range(const Key& lo, const Key& hi) override {
        lmdb_check(mdb_txn_renew(rtxn_), "txn_renew");
        MDB_cursor* cur;
        lmdb_check(mdb_cursor_open(rtxn_, dbi_, &cur), "cursor_open");
        MDB_val mk = val(lo), mv;
        size_t n = 0;
        std::string k, v;
        for (int rc = mdb_cursor_get(cur, &mk, &mv, MDB_SET_RANGE);
             rc == MDB_SUCCESS && key_le(static_cast<const uint8_t*>(mk.mv_data), mk.mv_size, hi);
             rc = mdb_cursor_get(cur, &mk, &mv, MDB_NEXT)) {
            k.assign(static_cast<const char*>(mk.mv_data), mk.mv_size);
            v.assign(static_cast<const char*>(mv.mv_data), mv.mv_size);
            n++;
        }
        mdb_cursor_close(cur);
        mdb_txn_reset(rtxn_);
        return n;
    }

    uint64_t used_bytes() override {
        MDB_envinfo info;
        MDB_stat    st;
        mdb_env_info(env_, &info);
        mdb_env_stat(env_, &st);
        return (info.me_last_pgno + 1) * uint64_t(st.ms_psize);
    }

private:
    template <class T> static MDB_val val(const T& c) {
        return {c.size(), const_cast<void*>(static_cast<const void*>(c.data()))};
    }
    MDB_env* env_  = nullptr;
    MDB_txn* rtxn_ = nullptr;
    MDB_dbi  dbi_  = 0;
};

std::unique_ptr<Store> make_lmdb_store() { return std::make_unique<LmdbStore>(); }
