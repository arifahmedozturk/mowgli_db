#include "server/repl.h"
#include "server/wire.h"
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <netdb.h>
#include <stdexcept>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

// ---- CRC32 (IEEE 802.3) ----

// Built on first use; a function-local static, so threads appending at the
// same time do not race to fill it.
static const uint32_t* crc32_table() {
    static const auto table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int j = 0; j < 8; j++)
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            t[i] = c;
        }
        return t;
    }();
    return table.data();
}

static uint32_t crc32_of(const void* data, size_t len) {
    const uint32_t* table = crc32_table();
    uint32_t crc = 0xFFFFFFFF;
    const auto* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; i++)
        crc = (crc >> 8) ^ table[(crc ^ p[i]) & 0xFF];
    return crc ^ 0xFFFFFFFF;
}

// ---- I/O helpers ----

// Write every byte of iov[0..n), resuming after short writes.
static bool writev_all(int fd, iovec* iov, int n) {
    while (n > 0) {
        ssize_t r = ::writev(fd, iov, n);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return false;
        auto done = static_cast<size_t>(r);
        while (n > 0 && done >= iov->iov_len) { done -= iov->iov_len; iov++; n--; }
        if (n > 0) {
            iov->iov_base = static_cast<char*>(iov->iov_base) + done;
            iov->iov_len -= done;
        }
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

ReplLog::ReplLog(std::string path, int fd, uint64_t next_lsn, uint64_t file_bytes)
    : path_(std::move(path)), fd_(fd), next_lsn_(next_lsn), file_bytes_(file_bytes) {}

ReplLog ReplLog::open(const std::string& path) {
    // Scan the existing log for the next LSN and the end of its last whole
    // record; a torn or corrupt tail is cut off (#123), or new records
    // appended after it would never reach a replica.
    uint64_t next = 1;
    off_t    good = 0;
    {
        int rfd = ::open(path.c_str(), O_RDONLY);
        if (rfd >= 0) {
            std::string cmd;
            while (true) {
                uint64_t lsn; uint32_t cmd_len, crc;
                if (read_exact(rfd, &lsn,     8) != 8) break;
                if (read_exact(rfd, &cmd_len, 4) != 4) break;
                if (read_exact(rfd, &crc,     4) != 4) break;
                struct stat st;
                if (::fstat(rfd, &st) != 0 || good + 16 + off_t(cmd_len) > st.st_size) break;
                cmd.resize(cmd_len);
                if (read_exact(rfd, cmd.data(), cmd_len) != ssize_t(cmd_len)) break;
                if (crc32_of(cmd.data(), cmd_len) != crc) break;
                good += 16 + off_t(cmd_len);
                if (lsn + 1 > next) next = lsn + 1;
            }
            ::close(rfd);
        }
    }

    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) throw std::runtime_error("repl: cannot open " + path);
    struct stat st;
    if (::fstat(fd, &st) == 0 && st.st_size > good && ::ftruncate(fd, good) != 0) {
        ::close(fd);
        throw std::runtime_error("repl: cannot cut the torn tail off " + path);
    }
    return ReplLog(path, fd, next, static_cast<uint64_t>(good));
}

ReplLog::~ReplLog() {
    if (fd_ >= 0) ::close(fd_);
}

ReplLog::ReplLog(ReplLog&& o) noexcept
    : path_(std::move(o.path_))
    , fd_(o.fd_)
    , next_lsn_(o.next_lsn_.load(std::memory_order_relaxed))
    , file_bytes_(o.file_bytes_)
    , failed_(o.failed_)
{
    o.fd_ = -1;
}

uint64_t ReplLog::append(const std::string& cmd) {
    uint32_t cmd_len = static_cast<uint32_t>(cmd.size());
    uint32_t crc     = crc32_of(cmd.data(), cmd.size());

    // The LSN is taken under the lock, so the file holds records in LSN
    // order. One writev per record; a failed write is cut off, and a failed
    // fdatasync fails this and every later append (#123).
    std::lock_guard<std::mutex> lk(append_mu_);
    if (failed_) throw std::runtime_error("repl: an earlier sync failed; restart the server");
    uint64_t lsn = next_lsn_.load(std::memory_order_relaxed);
    char head[16];
    memcpy(head,      &lsn,     8);
    memcpy(head + 8,  &cmd_len, 4);
    memcpy(head + 12, &crc,     4);
    iovec iov[2] = {{head, sizeof(head)}, {const_cast<char*>(cmd.data()), cmd.size()}};
    if (!writev_all(fd_, iov, cmd.empty() ? 1 : 2)) {
        int err = errno;
        if (::ftruncate(fd_, static_cast<off_t>(file_bytes_)) != 0) failed_ = true;
        throw std::runtime_error(std::string("repl: write failed: ") + std::strerror(err));
    }
    if (::fdatasync(fd_) != 0) {
        failed_ = true;
        throw std::runtime_error(std::string("repl: fdatasync failed: ") + std::strerror(errno));
    }
    file_bytes_ += sizeof(head) + cmd.size();
    next_lsn_.store(lsn + 1, std::memory_order_relaxed);
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
            // EOF — wait for a new append or stop signal. Timed against the
            // system clock: with the steady clock libstdc++ waits in
            // pthread_cond_clockwait, which GCC 11's ThreadSanitizer does not
            // intercept, so it misses the unlock during the wait and reports
            // every later lock of append_mu_ as a double lock (#114). A clock
            // jump only stretches or cuts short one 100 ms poll.
            std::unique_lock<std::mutex> lk(append_mu_);
            new_data_.wait_until(lk, std::chrono::system_clock::now() + std::chrono::milliseconds(100),
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

// ---- ReplServer ----

void ReplServer::start(int listen_fd, ReplLog& log) {
    stop_ = false;
    accept_thread_ = std::thread(&ReplServer::accept_loop, this, listen_fd, std::ref(log));
}

void ReplServer::stop() {
    stop_ = true;
    if (accept_thread_.joinable()) accept_thread_.join();
}

void ReplServer::accept_loop(int listen_fd, ReplLog& log) {
    std::vector<Feed> feeds;
    while (!stop_) {
        // Reap feeds whose replica has gone away.
        for (auto it = feeds.begin(); it != feeds.end();) {
            if (*it->done) { it->thread.join(); it = feeds.erase(it); }
            else ++it;
        }

        fd_set rfds; FD_ZERO(&rfds); FD_SET(listen_fd, &rfds);
        timeval tv{0, 100000};
        if (::select(listen_fd + 1, &rfds, nullptr, nullptr, &tv) <= 0) continue;
        int rfd = ::accept(listen_fd, nullptr, nullptr);
        if (rfd < 0) continue;

        // Read SYNC <lsn> handshake; a silent client must not block stop().
        timeval rto{2, 0};
        ::setsockopt(rfd, SOL_SOCKET, SO_RCVTIMEO, &rto, sizeof(rto));
        std::string msg;
        if (!recv_msg(rfd, msg) || msg.substr(0, 5) != "SYNC ") {
            ::close(rfd); continue;
        }
        uint64_t start_lsn = std::stoull(msg.substr(5));
        std::cerr << "[repl] replica connected, streaming from LSN " << start_lsn << "\n";

        auto done = std::make_shared<std::atomic<bool>>(false);
        feeds.push_back({std::thread([this, &log, rfd, start_lsn, done]() {
            log.feed_replica(rfd, start_lsn, stop_);
            std::cerr << "[repl] replica disconnected\n";
            *done = true;
        }), done});
    }
    for (auto& f : feeds) f.thread.join();
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
        FrameReader reader(fd);
        while (!stop_) {
            std::string msg;
            if (!reader.recv_msg(msg)) break;

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
