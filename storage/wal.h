#pragma once
#include <atomic>
#include <mutex>
#include <string>
#include <vector>

// Write-ahead log.
//
// Before any mutation the caller writes a CMD record (begin()) and receives a
// txn_id. After the mutation completes successfully the caller writes a COMMIT
// record (commit(txn_id)). On the next startup, recover() returns the commands
// from CMD records that have no matching COMMIT — i.e. the operations that were
// started but not confirmed before the crash.
//
// Wire format per record:
//   [payload_len : 4 LE]
//   [crc32       : 4 LE]   -- CRC of the payload bytes that follow
//   [tag         : 1]      -- 'W' = command, 'K' = commit
//   [txn_id      : 8 LE]
//   [cmd_bytes   : *]      -- only for 'W' records
class Wal {
public:
    static Wal open(const std::string& path);

    ~Wal();
    Wal(Wal&&) noexcept;
    Wal& operator=(Wal&&) = delete;

    // Write a CMD record and fdatasync. Returns a txn_id for commit().
    uint64_t begin(const std::string& cmd);

    // Write a COMMIT record and fdatasync.
    void commit(uint64_t txn_id);

    // Return uncommitted commands from a previous run, in original order.
    // Call truncate() after replaying them.
    std::vector<std::string> recover() const;

    // Truncate the WAL to zero (called after successful recovery).
    void truncate();

private:
    Wal(std::string path, int fd);

    void write_record(char tag, uint64_t txn_id,
                      const char* data, size_t data_len);

    std::string           path_;
    int                   fd_      = -1;
    std::atomic<uint64_t> next_txn_{1};
    std::mutex            write_mu_;
};
