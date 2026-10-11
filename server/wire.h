#pragma once
// Wire protocol helpers shared by server.cpp and client.cpp.
//
// Frame format:
//   "<len>\n<payload>\n"
//   len = byte count of payload, not including the trailing \n.

#include <algorithm>
#include <cstring>
#include <string>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>

// Largest accepted payload; bigger frames are rejected and the connection dropped.
static constexpr size_t MAX_FRAME_LEN  = 256u << 20;
// Longest accepted length header line (decimal digits).
static constexpr size_t MAX_HEADER_LEN = 20;

static bool send_all(int fd, const char* buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t r = ::write(fd, buf + sent, n - sent);
        if (r <= 0) return false;
        sent += static_cast<size_t>(r);
    }
    return true;
}

// Returns false on EOF/error or if the line exceeds max_len bytes.
static bool recv_line(int fd, std::string& out, size_t max_len) {
    out.clear();
    char c;
    while (true) {
        ssize_t r = ::read(fd, &c, 1);
        if (r <= 0) return false;
        if (c == '\n') return true;
        if (out.size() >= max_len) return false;
        out += c;
    }
}

// Send one framed message (command or response): header, payload and the
// trailing '\n' in a single writev, so the peer usually sees one read per frame.
static bool send_msg(int fd, const std::string& payload) {
    std::string header = std::to_string(payload.size()) + "\n";
    char nl = '\n';
    iovec iov[3] = {{header.data(), header.size()},
                    {const_cast<char*>(payload.data()), payload.size()},
                    {&nl, 1}};
    size_t total = header.size() + payload.size() + 1, sent = 0;
    int    first = 0;
    while (sent < total) {
        ssize_t r = ::writev(fd, iov + first, 3 - first);
        if (r <= 0) return false;
        sent += static_cast<size_t>(r);
        // Skip fully written iovecs and advance into a partially written one.
        for (size_t n = static_cast<size_t>(r); first < 3 && n > 0;) {
            size_t take = std::min(n, iov[first].iov_len);
            iov[first].iov_base = static_cast<char*>(iov[first].iov_base) + take;
            iov[first].iov_len -= take;
            n -= take;
            if (iov[first].iov_len == 0) first++;
        }
        while (first < 3 && iov[first].iov_len == 0) first++;
    }
    return true;
}

// Receive one framed message without buffering: reads byte-by-byte up to the
// header's '\n', so it never consumes bytes of a following frame. Fine for a
// single message on a fd (a handshake, one reply); use FrameReader for loops.
static bool recv_msg(int fd, std::string& out) {
    std::string len_str;
    if (!recv_line(fd, len_str, MAX_HEADER_LEN)) return false;
    if (len_str.empty() || !std::all_of(len_str.begin(), len_str.end(),
                                         [](char c) { return c >= '0' && c <= '9'; }))
        return false;
    size_t len;
    try { len = std::stoull(len_str); } catch (...) { return false; }
    if (len > MAX_FRAME_LEN) return false;

    // Grow as bytes arrive rather than trusting the header for the allocation.
    static constexpr size_t CHUNK = 64u << 10;
    out.clear();
    size_t got = 0;
    while (got < len) {
        out.resize(std::min(len, got + CHUNK));
        ssize_t r = ::read(fd, out.data() + got, out.size() - got);
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    // consume trailing \n
    char nl;
    [[maybe_unused]] ssize_t _nl = ::read(fd, &nl, 1);
    return true;
}

// Buffered frame reader for a connection that receives many frames: one
// read() can carry several frames, and bytes past the current frame are kept
// for the next call. Use one reader per fd, and don't mix it with the
// unbuffered recv_msg(fd) on the same fd.
class FrameReader {
public:
    explicit FrameReader(int fd) : fd_(fd), buf_(BUF_SIZE) {}

    bool recv_msg(std::string& out) {
        // Header: decimal length up to '\n'.
        const char* nl;
        while (!(nl = static_cast<const char*>(std::memchr(buf_.data() + beg_, '\n', end_ - beg_)))) {
            if (end_ - beg_ > MAX_HEADER_LEN || !fill()) return false;
        }
        const char* p = buf_.data() + beg_;
        if (p == nl || static_cast<size_t>(nl - p) > MAX_HEADER_LEN) return false;
        size_t len = 0;
        for (; p < nl; p++) {
            if (*p < '0' || *p > '9') return false;
            len = len * 10 + static_cast<size_t>(*p - '0');
            if (len > MAX_FRAME_LEN) return false;
        }
        beg_ = static_cast<size_t>(nl - buf_.data()) + 1;

        // Payload + trailing '\n': buffered bytes first, then the rest read
        // straight into out (exactly up to the frame end), growing as it arrives.
        const size_t total = len + 1;
        size_t have = std::min(total, end_ - beg_);
        out.assign(buf_.data() + beg_, have);
        beg_ += have;
        while (out.size() < total) {
            size_t got = out.size();
            out.resize(std::min(total, got + CHUNK));
            ssize_t r = ::read(fd_, out.data() + got, out.size() - got);
            if (r <= 0) return false;
            out.resize(got + static_cast<size_t>(r));
        }
        if (out.back() != '\n') return false;
        out.pop_back();
        return true;
    }

private:
    static constexpr size_t BUF_SIZE = 64u << 10;
    static constexpr size_t CHUNK    = 64u << 10;

    // Move unread bytes to the front, then read once into the free space.
    bool fill() {
        if (beg_ > 0) {
            std::memmove(buf_.data(), buf_.data() + beg_, end_ - beg_);
            end_ -= beg_;
            beg_  = 0;
        }
        ssize_t r = ::read(fd_, buf_.data() + end_, buf_.size() - end_);
        if (r <= 0) return false;
        end_ += static_cast<size_t>(r);
        return true;
    }

    int               fd_;
    std::vector<char> buf_;
    size_t            beg_ = 0, end_ = 0;  // unread bytes are buf_[beg_, end_)
};
