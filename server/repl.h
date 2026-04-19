#pragma once
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

// ---- Primary side ----
//
// Persistent append-only replication log: <data_dir>/repl.log
//
// Record format:
//   [lsn     : 8 LE]
//   [cmd_len : 4 LE]
//   [crc32   : 4 LE]   -- CRC of cmd bytes only
//   [cmd_bytes ...]
//
// One ReplLog per primary server process. After each successful mutation the
// server calls append(cmd). Replica connections call feed_replica(fd, lsn)
// which streams all existing entries from that LSN then blocks waiting for
// new ones (woken by append via condition_variable).
class ReplLog {
public:
    static ReplLog open(const std::string& path);
    ~ReplLog();
    ReplLog(ReplLog&&) noexcept;

    // Append a successfully committed command. Thread-safe, calls fdatasync.
    // Returns the LSN assigned to this entry.
    uint64_t append(const std::string& cmd);

    // Stream replication entries to a connected replica (starting from
    // start_lsn). Returns when the connection closes or stop is set.
    void feed_replica(int fd, uint64_t start_lsn, std::atomic<bool>& stop);

    uint64_t next_lsn() const {
        return next_lsn_.load(std::memory_order_relaxed);
    }

private:
    ReplLog(std::string path, int fd, uint64_t next_lsn);

    std::string            path_;
    int                    fd_     = -1;
    std::atomic<uint64_t>  next_lsn_{1};
    std::mutex             append_mu_;   // serialise writes + notify
    std::condition_variable new_data_;   // notified after each append
};

// ---- Replica side ----
//
// Connects to the primary's replication port, sends "SYNC <lsn>", then
// receives a stream of "<lsn> <cmd>" framed messages and replays them via
// on_cmd. The current LSN is persisted to lsn_path after each applied entry
// so reconnects can resume from the right position.
class ReplClient {
public:
    void start(const std::string& host, int port,
               std::function<void(const std::string&)> on_cmd,
               const std::string& lsn_path);
    void stop();
    ~ReplClient() { stop(); }

private:
    void loop(std::string host, int port,
              std::function<void(const std::string&)> on_cmd,
              std::string lsn_path);

    static uint64_t load_lsn(const std::string& path);
    static void     save_lsn(const std::string& path, uint64_t lsn);

    std::thread       thread_;
    std::atomic<bool> stop_{false};
};
