#include "storage/wal.h"
#include <cstring>
#include <fcntl.h>
#include <map>
#include <stdexcept>
#include <unistd.h>

// ---- CRC32 (IEEE 802.3 / Ethernet polynomial) ----

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

Wal::Wal(std::string path, int fd)
    : path_(std::move(path)), fd_(fd) {}

Wal Wal::open(const std::string& path) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0)
        throw std::runtime_error("wal: cannot open " + path);
    return Wal(path, fd);
}

Wal::~Wal() {
    if (fd_ >= 0) ::close(fd_);
}

Wal::Wal(Wal&& o) noexcept
    : path_(std::move(o.path_))
    , fd_(o.fd_)
    , next_txn_(o.next_txn_.load(std::memory_order_relaxed))
{
    o.fd_ = -1;
}

void Wal::write_record(char tag, uint64_t txn_id,
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
    write_all(fd_, &payload_len, 4);
    write_all(fd_, &crc,         4);
    write_all(fd_, payload.data(), payload_len);
    ::fdatasync(fd_);
}

uint64_t Wal::begin(const std::string& cmd) {
    uint64_t txn = next_txn_.fetch_add(1, std::memory_order_relaxed);
    write_record('W', txn, cmd.data(), cmd.size());
    return txn;
}

void Wal::commit(uint64_t txn_id) {
    write_record('K', txn_id, nullptr, 0);
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
    ::ftruncate(fd_, 0);
    ::fdatasync(fd_);
}
