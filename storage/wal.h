#pragma once
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

// Write-ahead log.
//
// Before any mutation the caller writes a CMD record (begin()) and receives a
// txn_id. After the mutation completes the caller writes a COMMIT record
// (commit(txn_id)). On the next startup, recover() returns the commands from
// CMD records that have no matching COMMIT — i.e. the operations that were
// started but not confirmed before the crash.
//
// Durability (#21): a record is in the OS page cache once written, which is
// enough to survive the process crashing. With Sync::COMMIT, commit() also
// waits until its records are on disk; concurrent commits share one
// fdatasync (group commit). With Sync::NONE it never syncs. begin() never
// syncs: its record only has to be on disk once the command is acknowledged,
// and the commit's sync covers it.
//
// Once no command is in flight and the log is over truncate_at bytes, it is
// emptied: every record in it is then a finished command.
//
// Wire format per record:
//   [payload_len : 4 LE]
//   [crc32       : 4 LE]   -- CRC of the payload bytes that follow
//   [tag         : 1]      -- 'W' = command, 'K' = commit
//   [txn_id      : 8 LE]
//   [cmd_bytes   : *]      -- only for 'W' records
class Wal {
public:
    enum class Sync { COMMIT, NONE };
    static constexpr size_t DEFAULT_TRUNCATE_AT = 16u << 20;

    static Wal open(const std::string& path, Sync sync = Sync::COMMIT,
                    size_t truncate_at = DEFAULT_TRUNCATE_AT);

    ~Wal();
    Wal(Wal&&) noexcept;
    Wal& operator=(Wal&&) = delete;

    // Write a CMD record. Returns a txn_id for commit().
    uint64_t begin(const std::string& cmd);

    // Write a COMMIT record and, with Sync::COMMIT, wait until it is on disk.
    void commit(uint64_t txn_id);

    // Return uncommitted commands from a previous run, in original order.
    // Call truncate() after replaying them.
    std::vector<std::string> recover() const;

    // Truncate the WAL to zero (called after successful recovery).
    void truncate();

    // fdatasync calls made by commits so far (for tests).
    uint64_t syncs() const { return syncs_.load(); }
    // Bytes in the log file now (for tests).
    uint64_t file_bytes() const { std::lock_guard<std::mutex> lk(write_mu_); return file_bytes_; }

private:
    Wal(std::string path, int fd, Sync sync, size_t truncate_at);

    // Append one record; returns the log position just past it.
    uint64_t write_record(char tag, uint64_t txn_id, const char* data, size_t data_len);
    // Wait until everything up to pos is on disk, syncing if no one else is.
    void     sync_to(uint64_t pos);

    std::string           path_;
    int                   fd_      = -1;
    Sync                  sync_;
    size_t                truncate_at_;
    std::atomic<uint64_t> next_txn_{1};

    mutable std::mutex    write_mu_;
    std::atomic<uint64_t> written_{0};   // bytes ever appended (survives truncation)
    uint64_t              file_bytes_ = 0;
    size_t                open_txns_  = 0;

    std::mutex              sync_mu_;
    std::condition_variable sync_cv_;
    uint64_t                synced_  = 0;   // everything before this position is on disk
    bool                    syncing_ = false;
    std::atomic<uint64_t>   syncs_{0};
};
