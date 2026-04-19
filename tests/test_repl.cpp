#include "mql/engine.h"
#include "server/repl.h"
#include "server/wire.h"
#include <arpa/inet.h>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

static int loopback_listen(int& out_port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in a{};
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port        = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
    ::listen(fd, 4);
    socklen_t len = sizeof(a);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    out_port = ntohs(a.sin_port);
    return fd;
}

// feed_replica streams entries that were written before the connection.
static void test_repllog_feed_existing() {
    const char* path = "/tmp/test_repl_a.log";
    std::remove(path);
    std::atomic<bool> stop{false};

    auto log = ReplLog::open(path);
    log.append("CMD_ONE");
    log.append("CMD_TWO");
    log.append("CMD_THREE");

    int fds[2];
    ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);

    std::thread feeder([&log, &stop, fd = fds[0]]() mutable {
        log.feed_replica(fd, 0, stop);
    });

    std::vector<std::string> got;
    for (int i = 0; i < 3; i++) {
        std::string msg;
        assert(recv_msg(fds[1], msg));
        got.push_back(msg);
    }

    stop = true;
    ::close(fds[1]);
    feeder.join();

    assert(got[0] == "1 CMD_ONE");
    assert(got[1] == "2 CMD_TWO");
    assert(got[2] == "3 CMD_THREE");

    std::remove(path);
}

// feed_replica wakes and streams entries appended after the connection.
static void test_repllog_feed_live() {
    const char* path = "/tmp/test_repl_b.log";
    std::remove(path);
    std::atomic<bool> stop{false};

    auto log = ReplLog::open(path);
    log.append("BEFORE");

    int fds[2];
    ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);

    std::thread feeder([&log, &stop, fd = fds[0]]() mutable {
        log.feed_replica(fd, 0, stop);
    });

    std::string msg1;
    assert(recv_msg(fds[1], msg1));
    assert(msg1 == "1 BEFORE");

    log.append("AFTER");

    std::string msg2;
    assert(recv_msg(fds[1], msg2));
    assert(msg2 == "2 AFTER");

    stop = true;
    ::close(fds[1]);
    feeder.join();

    std::remove(path);
}

// feed_replica respects start_lsn and skips already-applied entries.
static void test_repllog_resume_from_lsn() {
    const char* path = "/tmp/test_repl_e.log";
    std::remove(path);
    std::atomic<bool> stop{false};

    auto log = ReplLog::open(path);
    log.append("SKIP_ME");   // lsn 1
    log.append("KEEP_ME");   // lsn 2

    int fds[2];
    ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);

    std::thread feeder([&log, &stop, fd = fds[0]]() mutable {
        log.feed_replica(fd, 2, stop);  // start from lsn 2
    });

    std::string msg;
    assert(recv_msg(fds[1], msg));
    assert(msg == "2 KEEP_ME");

    stop = true;
    ::close(fds[1]);
    feeder.join();

    std::remove(path);
}

// ReplClient connects, sends SYNC, and receives + applies entries.
static void test_replclient_apply() {
    const char* log_path = "/tmp/test_repl_c.log";
    const char* lsn_path = "/tmp/test_repl_c.lsn";
    std::remove(log_path);
    std::remove(lsn_path);

    auto log = ReplLog::open(log_path);
    log.append("CMD_A");
    log.append("CMD_B");

    int port;
    int srv_fd = loopback_listen(port);
    std::atomic<bool> srv_stop{false};

    std::thread srv([srv_fd, &log, &srv_stop]() {
        fd_set rfds; FD_ZERO(&rfds); FD_SET(srv_fd, &rfds);
        timeval tv{2, 0};
        if (::select(srv_fd + 1, &rfds, nullptr, nullptr, &tv) <= 0) return;
        int fd = ::accept(srv_fd, nullptr, nullptr);
        if (fd < 0) return;
        std::string hs;
        recv_msg(fd, hs);
        uint64_t lsn = std::stoull(hs.substr(5));
        log.feed_replica(fd, lsn, srv_stop);
    });

    std::vector<std::string> applied;
    std::mutex               applied_mu;

    ReplClient client;
    client.start("127.0.0.1", port,
        [&applied, &applied_mu](const std::string& cmd) {
            std::lock_guard<std::mutex> lk(applied_mu);
            applied.push_back(cmd);
        },
        lsn_path);

    for (int i = 0; i < 100; i++) {
        {
            std::lock_guard<std::mutex> lk(applied_mu);
            if (applied.size() >= 2) break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    {
        std::lock_guard<std::mutex> lk(applied_mu);
        assert(applied.size() >= 2);
        assert(applied[0] == "CMD_A");
        assert(applied[1] == "CMD_B");
    }

    srv_stop = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    ::close(srv_fd);
    srv.join();
    client.stop();

    std::remove(log_path);
    std::remove(lsn_path);
}

// End-to-end: mutations on primary are replicated to replica Engine.
static void test_engine_replication() {
    const char* p_dir    = "/tmp/test_repl_primary";
    const char* r_dir    = "/tmp/test_repl_replica";
    const char* log_path = "/tmp/test_repl_d.log";
    const char* lsn_path = "/tmp/test_repl_d.lsn";

    std::filesystem::remove_all(p_dir);
    std::filesystem::remove_all(r_dir);
    std::remove(log_path);
    std::remove(lsn_path);
    std::filesystem::create_directories(p_dir);
    std::filesystem::create_directories(r_dir);

    Engine primary(p_dir);
    auto   repl_log = ReplLog::open(log_path);
    Engine replica(r_dir);

    auto primary_exec = [&](const std::string& cmd) {
        primary.exec(cmd);
        repl_log.append(cmd);
    };

    int port;
    int srv_fd = loopback_listen(port);
    std::atomic<bool> srv_stop{false};
    std::vector<std::thread> feed_threads;

    std::thread srv([srv_fd, &repl_log, &srv_stop, &feed_threads]() {
        while (!srv_stop) {
            fd_set rfds; FD_ZERO(&rfds); FD_SET(srv_fd, &rfds);
            timeval tv{0, 200000};
            if (::select(srv_fd + 1, &rfds, nullptr, nullptr, &tv) <= 0) continue;
            int fd = ::accept(srv_fd, nullptr, nullptr);
            if (fd < 0) continue;
            std::string hs;
            recv_msg(fd, hs);
            uint64_t lsn = std::stoull(hs.substr(5));
            feed_threads.emplace_back([fd, lsn, &repl_log, &srv_stop]() {
                repl_log.feed_replica(fd, lsn, srv_stop);
            });
        }
    });

    ReplClient client;
    client.start("127.0.0.1", port,
        [&replica](const std::string& cmd) { replica.exec(cmd); },
        lsn_path);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    primary_exec("TABLE users(id string PRIMARY KEY, name string)");
    primary_exec("UPDATE(users, 'alice', 'wonderland')");
    primary_exec("UPDATE(users, 'bob', 'builder')");

    for (int i = 0; i < 100; i++) {
        try {
            if (replica.exec("QUERY(users, 'bob')") != "NOT FOUND") break;
        } catch (...) {}
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    assert(replica.exec("QUERY(users, 'alice')") == "alice | wonderland");
    assert(replica.exec("QUERY(users, 'bob')")   == "bob | builder");

    srv_stop = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    ::close(srv_fd);
    srv.join();
    for (auto& t : feed_threads)
        if (t.joinable()) t.join();
    client.stop();

    std::filesystem::remove_all(p_dir);
    std::filesystem::remove_all(r_dir);
    std::remove(log_path);
    std::remove(lsn_path);
}

int main() {
    test_repllog_feed_existing();
    test_repllog_feed_live();
    test_repllog_resume_from_lsn();
    test_replclient_apply();
    test_engine_replication();
    return 0;
}
