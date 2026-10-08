#include "server/wire.h"
#include <cassert>
#include <cstdio>
#include <string>
#include <sys/socket.h>
#include <thread>

// Connected socket pair: write raw bytes to fds[0], read frames from fds[1].
struct Pair {
    int fds[2];
    Pair()  { assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0); }
    ~Pair() { close(fds[0]); close(fds[1]); }
    void send_raw(const std::string& s) { assert(send_all(fds[0], s.data(), s.size())); }
    void close_writer() { shutdown(fds[0], SHUT_WR); }
};

static void test_roundtrip() {
    Pair p;
    assert(send_msg(p.fds[0], "QUERY(users, 'alice')"));
    assert(send_msg(p.fds[0], ""));
    std::string out;
    assert(recv_msg(p.fds[1], out) && out == "QUERY(users, 'alice')");
    assert(recv_msg(p.fds[1], out) && out.empty());
}

static void test_large_payload_in_pieces() {
    Pair p;
    std::string payload(1u << 20, 'x');
    std::thread w([&] { assert(send_msg(p.fds[0], payload)); });
    std::string out;
    assert(recv_msg(p.fds[1], out));
    w.join();
    assert(out == payload);
}

static void test_rejects_length_over_cap() {
    Pair p;
    p.send_raw(std::to_string(MAX_FRAME_LEN + 1) + "\n");
    std::string out;
    assert(!recv_msg(p.fds[1], out));
}

static void test_rejects_huge_length() {
    // Used to throw std::length_error from resize() and terminate the server.
    Pair p;
    p.send_raw("99999999999999999999\n");
    std::string out;
    assert(!recv_msg(p.fds[1], out));
}

static void test_rejects_non_numeric_length() {
    for (const char* hdr : {"-1\n", "abc\n", "12abc\n", "\n", " 5\n"}) {
        Pair p;
        p.send_raw(hdr);
        p.close_writer();
        std::string out;
        assert(!recv_msg(p.fds[1], out));
    }
}

static void test_rejects_endless_header() {
    // A header with no newline must not be buffered forever.
    Pair p;
    p.send_raw(std::string(MAX_HEADER_LEN + 1, '1'));
    std::string out;
    assert(!recv_msg(p.fds[1], out));
}

static void test_truncated_payload() {
    Pair p;
    p.send_raw("10\nabc");
    p.close_writer();
    std::string out;
    assert(!recv_msg(p.fds[1], out));
}

int main() {
    test_roundtrip();
    test_large_payload_in_pieces();
    test_rejects_length_over_cap();
    test_rejects_huge_length();
    test_rejects_non_numeric_length();
    test_rejects_endless_header();
    test_truncated_payload();
    std::puts("test_wire: all passed");
    return 0;
}
