#include "mql/engine.h"
#include "server/wire.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// ---- connection handler ----

static void handle_conn(int conn_fd, Engine& engine) {
    while (true) {
        std::string cmd;
        if (!recv_msg(conn_fd, cmd)) break;

        if (cmd == "exit" || cmd == "quit") {
            send_msg(conn_fd, "BYE");
            break;
        }

        engine.inflight.fetch_add(1, std::memory_order_relaxed);
        std::string result;
        try {
            result = engine.exec(cmd);
        } catch (const std::exception& e) {
            result = std::string("ERROR: ") + e.what();
        }
        engine.inflight.fetch_sub(1, std::memory_order_relaxed);

        if (!send_msg(conn_fd, result)) break;
    }
    ::close(conn_fd);
}

// ---- background compaction thread ----
// Fires compact_all() when the server has been idle (inflight == 0) for
// IDLE_SECONDS consecutive seconds, then waits COOLDOWN_SECONDS before
// the next compaction.

static constexpr int IDLE_SECONDS     = 30;
static constexpr int COOLDOWN_SECONDS = 300;

static void compact_loop(Engine& engine, std::atomic<bool>& stop,
                         std::mutex& cv_mtx, std::condition_variable& cv) {
    using namespace std::chrono;
    while (true) {
        {
            std::unique_lock<std::mutex> lk(cv_mtx);
            cv.wait_for(lk, seconds(IDLE_SECONDS),
                        [&]{ return stop.load(); });
        }
        if (stop) break;

        if (engine.inflight.load(std::memory_order_relaxed) > 0)
            continue;

        std::cerr << "[compact] idle for " << IDLE_SECONDS
                  << "s — starting compaction\n";
        auto t0 = steady_clock::now();
        engine.compact_all();
        double secs = duration<double>(steady_clock::now() - t0).count();
        std::cerr << "[compact] done in " << secs << "s\n";

        // cooldown — wait before checking idle again
        {
            std::unique_lock<std::mutex> lk(cv_mtx);
            cv.wait_for(lk, seconds(COOLDOWN_SECONDS),
                        [&]{ return stop.load(); });
        }
        if (stop) break;
    }
}

// ---- shutdown helpers ----

static std::atomic<bool>        g_stop{false};
static std::condition_variable* g_cv_ptr = nullptr;
static std::mutex*              g_cv_mtx_ptr = nullptr;

static void sig_handler(int) {
    g_stop = true;
    if (g_cv_ptr && g_cv_mtx_ptr) {
        std::lock_guard<std::mutex> lk(*g_cv_mtx_ptr);
        g_cv_ptr->notify_all();
    }
}

// ---- main ----

int main(int argc, char* argv[]) {
    int         port     = 5432;
    std::string data_dir = "./data";

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--port" && i + 1 < argc) { port = std::stoi(argv[++i]); }
        else if (a == "--data" && i + 1 < argc) { data_dir = argv[++i]; }
        else { data_dir = a; }
    }

    std::filesystem::create_directories(data_dir);
    std::cerr << "heavy-trie server  port=" << port
              << "  data=" << data_dir << "\n";

    Engine engine(data_dir);

    // setup signal handlers
    std::mutex              cv_mtx;
    std::condition_variable cv;
    g_cv_ptr     = &cv;
    g_cv_mtx_ptr = &cv_mtx;
    std::signal(SIGINT,  sig_handler);
    std::signal(SIGTERM, sig_handler);

    // background compaction thread
    std::thread compact_thread(compact_loop,
                               std::ref(engine), std::ref(g_stop),
                               std::ref(cv_mtx), std::ref(cv));

    // TCP listener
    int srv_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (srv_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    ::setsockopt(srv_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(port));

    if (::bind(srv_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    ::listen(srv_fd, 64);
    std::cerr << "listening on port " << port << "\n";

    std::vector<std::thread> workers;

    while (!g_stop) {
        // Non-blocking accept poll so we can notice g_stop.
        fd_set rfds; FD_ZERO(&rfds); FD_SET(srv_fd, &rfds);
        timeval tv{1, 0};
        int sel = ::select(srv_fd + 1, &rfds, nullptr, nullptr, &tv);
        if (sel <= 0) continue;

        int conn_fd = ::accept(srv_fd, nullptr, nullptr);
        if (conn_fd < 0) continue;

        workers.emplace_back([conn_fd, &engine]() {
            handle_conn(conn_fd, engine);
        });

        // Clean up finished threads.
        workers.erase(
            std::remove_if(workers.begin(), workers.end(),
                           [](std::thread& th){
                               if (th.joinable() &&
                                   th.get_id() != std::this_thread::get_id()) {
                                   // non-blocking join attempt via native handle
                                   // just detach; OS will clean up on exit
                                   th.detach();
                                   return true;
                               }
                               return false;
                           }),
            workers.end());
    }

    ::close(srv_fd);

    // Wait for in-flight requests to drain (up to 5s).
    for (int i = 0; i < 50 && engine.inflight.load() > 0; i++)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    {
        std::lock_guard<std::mutex> lk(cv_mtx);
        g_stop = true;
        cv.notify_all();
    }
    compact_thread.join();

    for (auto& th : workers)
        if (th.joinable()) th.detach();

    std::cerr << "server stopped\n";
    return 0;
}
