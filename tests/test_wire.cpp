#include "server/wire.h"
#include <cassert>
#include <chrono>
#include <chrono>
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

// ---- FrameReader (buffered) ----

// Many frames arriving in one write are all decoded, in order.
static void test_reader_back_to_back_frames() {
    Pair p;
    p.send_raw("3\nabc\n0\n\n12\nhello world!\n");
    FrameReader r(p.fds[1]);
    std::string out;
    assert(r.recv_msg(out) && out == "abc");
    assert(r.recv_msg(out) && out.empty());
    assert(r.recv_msg(out) && out == "hello world!");
}

// Header and payload trickling in a few bytes at a time.
static void test_reader_frame_split_across_writes() {
    Pair p;
    std::thread w([&] {
        for (const char* part : {"1", "1\nhel", "lo", " world", "\n", "2\nok", "\n"}) {
            p.send_raw(part);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });
    FrameReader r(p.fds[1]);
    std::string out;
    assert(r.recv_msg(out) && out == "hello world");
    assert(r.recv_msg(out) && out == "ok");
    w.join();
}

// Large payloads (bigger than the reader's buffer) between small frames.
static void test_reader_large_and_small_frames() {
    Pair p;
    std::string big(3u << 20, 'x');
    big[12345] = 'y';
    std::thread w([&] {
        assert(send_msg(p.fds[0], "a"));
        assert(send_msg(p.fds[0], big));
        assert(send_msg(p.fds[0], "b"));
    });
    FrameReader r(p.fds[1]);
    std::string out;
    assert(r.recv_msg(out) && out == "a");
    assert(r.recv_msg(out) && out == big);
    assert(r.recv_msg(out) && out == "b");
    w.join();
}

// 10K frames streamed back to back.
static void test_reader_stream() {
    Pair p;
    const int N = 10000;
    std::thread w([&] {
        for (int i = 0; i < N; i++) assert(send_msg(p.fds[0], std::to_string(i)));
    });
    FrameReader r(p.fds[1]);
    std::string out;
    for (int i = 0; i < N; i++) assert(r.recv_msg(out) && out == std::to_string(i));
    w.join();
}

static void test_reader_rejects_bad_frames() {
    for (const char* raw : {"-1\n", "12abc\n", "\n", "3\nabcX", "99999999999999999999\n"}) {
        Pair p;
        p.send_raw(raw);
        p.close_writer();
        FrameReader r(p.fds[1]);
        std::string out;
        assert(!r.recv_msg(out));
    }
    Pair p;  // endless header, writer still open
    p.send_raw(std::string(MAX_HEADER_LEN + 5, '1'));
    FrameReader r(p.fds[1]);
    std::string out;
    assert(!r.recv_msg(out));
}

int main() {
    test_roundtrip();
    test_large_payload_in_pieces();
    test_rejects_length_over_cap();
    test_rejects_huge_length();
    test_rejects_non_numeric_length();
    test_rejects_endless_header();
    test_truncated_payload();
    test_reader_back_to_back_frames();
    test_reader_frame_split_across_writes();
    test_reader_large_and_small_frames();
    test_reader_stream();
    test_reader_rejects_bad_frames();
    std::puts("test_wire: all passed");
    return 0;
}
