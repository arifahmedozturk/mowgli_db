#include "storage/wal.h"
#include <algorithm>
#include <array>
#include <cstring>
#include <fcntl.h>
#include <map>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

// ---- CRC32 (IEEE 802.3 / Ethernet polynomial) ----

// Built on first use; a function-local static, so threads committing at the
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
    const uint32_t* crc32_table = ::crc32_table();
    uint32_t crc = 0xFFFFFFFF;
    const auto* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; i++)
        crc = (crc >> 8) ^ crc32_table[(crc ^ p[i]) & 0xFF];
    return crc ^ 0xFFFFFFFF;
}

// ---- helpers ----

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

static bool read_all(int fd, void* buf, size_t n) {
    auto* p = static_cast<char*>(buf);
    size_t done = 0;
    while (done < n) {
        ssize_t r = ::read(fd, p + done, n - done);
        if (r <= 0) return false;
        done += static_cast<size_t>(r);
    }
    return true;
}

// ---- Wal ----

Wal::Wal(std::string path, int fd, Sync sync, size_t truncate_at)
    : path_(std::move(path)), fd_(fd), sync_(sync), truncate_at_(truncate_at) {
    struct stat st;
    if (::fstat(fd_, &st) == 0) file_bytes_ = static_cast<uint64_t>(st.st_size);
}

Wal Wal::open(const std::string& path, Sync sync, size_t truncate_at) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0)
        throw std::runtime_error("wal: cannot open " + path);
    return Wal(path, fd, sync, truncate_at);
}

Wal::~Wal() {
    if (fd_ >= 0) ::close(fd_);
}

Wal::Wal(Wal&& o) noexcept
    : path_(std::move(o.path_))
    , fd_(o.fd_)
    , sync_(o.sync_)
    , truncate_at_(o.truncate_at_)
    , next_txn_(o.next_txn_.load(std::memory_order_relaxed))
    , written_(o.written_.load())
    , file_bytes_(o.file_bytes_)
    , open_txns_(o.open_txns_)
    , synced_(o.synced_)
{
    o.fd_ = -1;
}

uint64_t Wal::write_record(char tag, uint64_t txn_id,
                           const char* data, size_t data_len) {
    // payload = tag(1) + txn_id(8) + data(data_len)
    uint32_t payload_len = static_cast<uint32_t>(1 + 8 + data_len);

    std::vector<char> payload(payload_len);
    payload[0] = tag;
    memcpy(payload.data() + 1, &txn_id, 8);
    if (data_len > 0)
        memcpy(payload.data() + 9, data, data_len);

    uint32_t crc = crc32_of(payload.data(), payload_len);

    std::lock_guard<std::mutex> lk(write_mu_);
    if (tag == 'W') open_txns_++;
    write_all(fd_, &payload_len, 4);
    write_all(fd_, &crc,         4);
    write_all(fd_, payload.data(), payload_len);
    file_bytes_ += 8 + payload_len;
    uint64_t end = written_.fetch_add(8 + payload_len) + 8 + payload_len;
    if (tag == 'K') {
        if (open_txns_ > 0) open_txns_--;
        // Nothing in flight: every record is a finished command.
        if (open_txns_ == 0 && file_bytes_ > truncate_at_) {
            ::ftruncate(fd_, 0);
            file_bytes_ = 0;
        }
    }
    return end;
}

void Wal::sync_to(uint64_t pos) {
    std::unique_lock<std::mutex> lk(sync_mu_);
    while (synced_ < pos) {
        if (syncing_) { sync_cv_.wait(lk); continue; }
        // Lead a sync for everything written so far, so commits that arrive
        // while it runs are covered by the next one, not one each.
        syncing_ = true;
        uint64_t target = written_.load();
        lk.unlock();
        ::fdatasync(fd_);
        syncs_++;
        lk.lock();
        synced_  = std::max(synced_, target);
        syncing_ = false;
        sync_cv_.notify_all();
    }
}

uint64_t Wal::begin(const std::string& cmd) {
    uint64_t txn = next_txn_.fetch_add(1, std::memory_order_relaxed);
    write_record('W', txn, cmd.data(), cmd.size());
    return txn;
}

void Wal::commit(uint64_t txn_id) {
    uint64_t end = write_record('K', txn_id, nullptr, 0);
    if (sync_ == Sync::COMMIT) sync_to(end);
}

std::vector<std::string> Wal::recover() const {
    int rfd = ::open(path_.c_str(), O_RDONLY);
    if (rfd < 0) return {};

    // Use an ordered map so uncommitted commands replay in original txn_id order.
    std::map<uint64_t, std::string> pending;

    while (true) {
        uint32_t payload_len, stored_crc;
        if (!read_all(rfd, &payload_len, 4)) break;
        if (!read_all(rfd, &stored_crc,  4)) break;
        if (payload_len < 9) break; // minimum: tag(1) + txn_id(8)

        std::vector<char> payload(payload_len);
        if (!read_all(rfd, payload.data(), payload_len)) break;

        if (crc32_of(payload.data(), payload_len) != stored_crc)
            break; // torn write at end of file — stop here

        char     tag = payload[0];
        uint64_t txn_id;
        memcpy(&txn_id, payload.data() + 1, 8);

        if (tag == 'W') {
            std::string cmd(payload.data() + 9, payload_len - 9);
            pending.emplace(txn_id, std::move(cmd));
        } else if (tag == 'K') {
            pending.erase(txn_id);
        }
    }

    ::close(rfd);

    std::vector<std::string> out;
    out.reserve(pending.size());
    for (auto& [id, cmd] : pending)
        out.push_back(std::move(cmd));
    return out;
}

void Wal::truncate() {
    // With O_APPEND, ftruncate resets file size; next write goes to offset 0.
    std::lock_guard<std::mutex> lk(write_mu_);
    ::ftruncate(fd_, 0);
    ::fdatasync(fd_);
    file_bytes_ = 0;
}
