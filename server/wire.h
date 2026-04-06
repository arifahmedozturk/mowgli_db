#pragma once
// Wire protocol helpers shared by server.cpp and client.cpp.
//
// Frame format:
//   "<len>\n<payload>\n"
//   len = byte count of payload, not including the trailing \n.

#include <string>
#include <unistd.h>

static bool send_all(int fd, const char* buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t r = ::write(fd, buf + sent, n - sent);
        if (r <= 0) return false;
        sent += static_cast<size_t>(r);
    }
    return true;
}

static bool recv_line(int fd, std::string& out) {
    out.clear();
    char c;
    while (true) {
        ssize_t r = ::read(fd, &c, 1);
        if (r <= 0) return false;
        if (c == '\n') return true;
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
    if (!recv_line(fd, len_str)) return false;
    size_t len;
    try { len = std::stoull(len_str); } catch (...) { return false; }
    out.resize(len);
    size_t got = 0;
    while (got < len) {
        ssize_t r = ::read(fd, out.data() + got, len - got);
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    // consume trailing \n
    char nl;
    [[maybe_unused]] ssize_t _nl = ::read(fd, &nl, 1);
    return true;
}
