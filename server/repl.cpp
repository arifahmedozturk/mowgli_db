#include "server/repl.h"
#include "server/wire.h"
#include <arpa/inet.h>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <netdb.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

// ---- CRC32 (IEEE 802.3) ----

static uint32_t crc32_table[256];

static void init_crc32() {
    static bool ready = false;
    if (ready) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc32_table[i] = c;
    }
    ready = true;
}

static uint32_t crc32_of(const void* data, size_t len) {
    init_crc32();
    uint32_t crc = 0xFFFFFFFF;
    const auto* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; i++)
        crc = (crc >> 8) ^ crc32_table[(crc ^ p[i]) & 0xFF];
    return crc ^ 0xFFFFFFFF;
}

// ---- I/O helpers ----

static bool write_all(int fd, const void* buf, size_t n) {
    const auto* p = static_cast<const char*>(buf);
    size_t done = 0;
    while (done < n) {
        ssize_t r = ::write(fd, p + done, n - done);
        if (r <= 0) return false;
        done += static_cast<size_t>(r);
    }
    return true;
}

// Read exactly n bytes. Returns n on success, 0 on clean EOF before any byte,
// -1 on error or partial EOF mid-record.
static ssize_t read_exact(int fd, void* buf, size_t n) {
    auto* p = static_cast<char*>(buf);
    size_t done = 0;
    while (done < n) {
        ssize_t r = ::read(fd, p + done, n - done);
        if (r == 0) return done == 0 ? 0 : -1; // clean EOF vs partial
        if (r < 0)  return -1;
        done += static_cast<size_t>(r);
    }
    return static_cast<ssize_t>(n);
}

// ---- ReplLog ----

ReplLog::ReplLog(std::string path, int fd, uint64_t next_lsn)
    : path_(std::move(path)), fd_(fd), next_lsn_(next_lsn) {}

ReplLog ReplLog::open(const std::string& path) {
    // Scan existing log to find the next LSN.
    uint64_t next = 1;
    {
        int rfd = ::open(path.c_str(), O_RDONLY);
        if (rfd >= 0) {
            while (true) {
                uint64_t lsn; uint32_t cmd_len, crc;
                if (read_exact(rfd, &lsn,     8) != 8) break;
                if (read_exact(rfd, &cmd_len,  4) != 4) break;
                if (read_exact(rfd, &crc,      4) != 4) break;
                if (::lseek(rfd, static_cast<off_t>(cmd_len), SEEK_CUR) < 0) break;
                if (lsn + 1 > next) next = lsn + 1;
            }
            ::close(rfd);
        }
    }

    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) throw std::runtime_error("repl: cannot open " + path);
    return ReplLog(path, fd, next);
}

ReplLog::~ReplLog() {
    if (fd_ >= 0) ::close(fd_);
}

ReplLog::ReplLog(ReplLog&& o) noexcept
    : path_(std::move(o.path_))
    , fd_(o.fd_)
    , next_lsn_(o.next_lsn_.load(std::memory_order_relaxed))
{
    o.fd_ = -1;
}

uint64_t ReplLog::append(const std::string& cmd) {
    uint64_t lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed);
    uint32_t cmd_len = static_cast<uint32_t>(cmd.size());
    uint32_t crc     = crc32_of(cmd.data(), cmd.size());

    std::lock_guard<std::mutex> lk(append_mu_);
    write_all(fd_, &lsn,     8);
    write_all(fd_, &cmd_len, 4);
    write_all(fd_, &crc,     4);
    write_all(fd_, cmd.data(), cmd.size());
    ::fdatasync(fd_);
    new_data_.notify_all();
    return lsn;
}

void ReplLog::feed_replica(int fd, uint64_t start_lsn, std::atomic<bool>& stop) {
    int rfd = ::open(path_.c_str(), O_RDONLY);
    if (rfd < 0) { ::close(fd); return; }

    while (!stop) {
        uint64_t lsn; uint32_t cmd_len, stored_crc;

        ssize_t r = read_exact(rfd, &lsn, 8);
        if (r == 0) {
            // EOF — wait for a new append or stop signal
            std::unique_lock<std::mutex> lk(append_mu_);
            new_data_.wait_for(lk, std::chrono::milliseconds(100),
                               [&]{ return stop.load(); });
            continue;
        }
        if (r < 0) break;

        if (read_exact(rfd, &cmd_len,    4) != 4) break;
        if (read_exact(rfd, &stored_crc, 4) != 4) break;

        std::string cmd(cmd_len, '\0');
        if (read_exact(rfd, cmd.data(), cmd_len) != (ssize_t)cmd_len) break;

        if (crc32_of(cmd.data(), cmd.size()) != stored_crc) break; // corruption

        if (lsn < start_lsn) continue; // skip already-applied entries

        // Send "<lsn> <cmd>" to the replica
        if (!send_msg(fd, std::to_string(lsn) + " " + cmd)) break;
    }

    ::close(rfd);
    ::close(fd);
}

// ---- ReplClient ----

uint64_t ReplClient::load_lsn(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return 0;
    uint64_t lsn = 0;
    f.read(reinterpret_cast<char*>(&lsn), 8);
    return lsn;
}

void ReplClient::save_lsn(const std::string& path, uint64_t lsn) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    f.write(reinterpret_cast<const char*>(&lsn), 8);
}

void ReplClient::start(const std::string& host, int port,
                        std::function<void(const std::string&)> on_cmd,
                        const std::string& lsn_path) {
    thread_ = std::thread([this, host, port,
                           on_cmd = std::move(on_cmd), lsn_path]() mutable {
        loop(std::move(host), port, std::move(on_cmd), std::move(lsn_path));
    });
}

void ReplClient::stop() {
    stop_ = true;
    if (thread_.joinable()) thread_.join();
}

void ReplClient::loop(std::string host, int port,
                       std::function<void(const std::string&)> on_cmd,
                       std::string lsn_path) {
    while (!stop_) {
        uint64_t lsn = load_lsn(lsn_path);

        // Connect to primary replication port
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::seconds(2)); continue; }

        struct addrinfo hints{}, *res = nullptr;
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(),
                          &hints, &res) != 0 || !res) {
            ::close(fd);
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }
        bool ok = ::connect(fd, res->ai_addr, res->ai_addrlen) == 0;
        ::freeaddrinfo(res);
        if (!ok) {
            ::close(fd);
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }

        // Handshake: tell primary where to start
        if (!send_msg(fd, "SYNC " + std::to_string(lsn))) {
            ::close(fd); continue;
        }

        std::cerr << "[repl] connected to primary, resuming from LSN " << lsn << "\n";

        // Stream incoming entries
        while (!stop_) {
            std::string msg;
            if (!recv_msg(fd, msg)) break;

            // Parse "<lsn> <cmd>"
            auto sp = msg.find(' ');
            if (sp == std::string::npos) break;
            uint64_t entry_lsn;
            try { entry_lsn = std::stoull(msg.substr(0, sp)); } catch (...) { break; }
            std::string cmd = msg.substr(sp + 1);

            try { on_cmd(cmd); } catch (...) {}

            lsn = entry_lsn + 1;
            save_lsn(lsn_path, lsn);
        }

        ::close(fd);
        if (!stop_) {
            std::cerr << "[repl] lost connection to primary, retrying in 2s\n";
            for (int i = 0; i < 200 && !stop_; i++)
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}
