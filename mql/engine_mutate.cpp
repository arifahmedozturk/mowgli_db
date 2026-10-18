#include "mql/engine.h"
#include <string>

// Mutations are logged to the WAL before execution and committed after; the
// tables commit their changes first (#28), with or without the WAL.

bool Engine::is_mutation(const std::string& kw) {
    return kw == "NEW" || kw == "DELETE" || kw == "UPDATE"
        || kw == "BULK" || kw == "TABLE";
}

std::string Engine::exec_mutation(const std::string& cmd, std::vector<Token>& t, size_t& i) {
    const std::string kw = t[i].value;
    uint64_t txn = wal_on_ ? wal_.begin(cmd) : 0;
    // A command that fails is finished too: whatever it changed before
    // failing is committed and its log record closed, so it is neither left
    // open (keeping the log from being emptied) nor replayed on the next start.
    auto finish = [&] {
        commit_tables();
        if (wal_on_) wal_.commit(txn);
    };
    std::string result;
    try {
        if      (kw == "TABLE")  result = exec_table (t, i);
        else if (kw == "NEW")    result = exec_new   (t, i);
        else if (kw == "UPDATE") result = exec_update(t, i);
        else if (kw == "DELETE") result = exec_delete(t, i);
        else                     result = exec_bulk  (t, i);
    } catch (...) {
        finish();
        throw;
    }
    finish();
    return result;
}
