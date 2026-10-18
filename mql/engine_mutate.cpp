#include "mql/engine.h"
#include <string>

// Writes are logged to the WAL before they run and committed after; the
// tables commit their changes first (#28), with or without the WAL.

std::string Engine::exec_mutation(const std::string& cmd, const Statement& s) {
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
        result = run(s);
    } catch (...) {
        finish();
        throw;
    }
    finish();
    return result;
}
