#pragma once
// Wire protocol helpers shared by server.cpp and client.cpp.
//
// Frame format:
//   "<len>\n<payload>\n"
//   len = byte count of payload, not including the trailing \n.

#include <algorithm>
#include <string>
#include <unistd.h>

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

// Send one framed message (command or response).
static bool send_msg(int fd, const std::string& payload) {
    std::string header = std::to_string(payload.size()) + "\n";
    std::string body   = payload + "\n";
    return send_all(fd, header.c_str(), header.size())
        && send_all(fd, body.c_str(),   body.size());
}

// Receive one framed message (command or response).
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
