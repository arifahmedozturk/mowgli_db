#include "mql/engine.h"
#include "server/cluster.h"
#include "server/repl.h"
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

// ---- cluster scatter/gather helpers ----

// Parse "(N rows)" trailer from a RANGE response, return the rows prefix and count.
static std::pair<std::string, size_t> strip_range_trailer(const std::string& s) {
    if (s == "NOT FOUND") return {"", 0};
    size_t paren = s.rfind('(');
    if (paren == std::string::npos) return {s, 0};
    size_t count = 0;
    try { count = std::stoull(s.substr(paren + 1)); } catch (...) {}
    return {s.substr(0, paren), count};
}

static std::string merge_range_results(const std::string& a, const std::string& b) {
    auto [a_rows, a_count] = strip_range_trailer(a);
    auto [b_rows, b_count] = strip_range_trailer(b);
    size_t total = a_count + b_count;
    if (total == 0) return "NOT FOUND";
    return a_rows + b_rows + "(" + std::to_string(total) + " rows)";
}

// Returns true for mutations that succeeded (should be replicated).
static bool should_replicate(const std::string& cmd, const std::string& result) {
    auto end = cmd.find_first_of("( ");
    std::string kw = cmd.substr(0, end == std::string::npos ? cmd.size() : end);
    for (auto& c : kw) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    if (kw != "NEW" && kw != "DELETE" && kw != "UPDATE" && kw != "BULK" && kw != "TABLE")
        return false;
    // Replicate only on success (not DUPLICATE KEY / NOT FOUND)
    return result == "OK" || (!result.empty() && std::isdigit(static_cast<unsigned char>(result[0])));
}

static std::string route_and_exec(const std::string& cmd, Engine& engine,
                                   const Cluster& cluster, ReplLog* repl_log,
                                   const std::string& primary_addr) {
    // Replica: forward writes to primary rather than executing locally.
    if (!primary_addr.empty()) {
        auto end = cmd.find_first_of("( ");
        std::string kw = cmd.substr(0, end == std::string::npos ? cmd.size() : end);
        for (auto& c : kw) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        bool is_mut = kw == "NEW" || kw == "DELETE" || kw == "UPDATE"
                   || kw == "BULK" || kw == "TABLE";
        if (is_mut) {
            auto colon = primary_addr.rfind(':');
            std::string host = primary_addr.substr(0, colon);
            int port = std::stoi(primary_addr.substr(colon + 1));
            PeerInfo p; p.host = host; p.port = port;
            return Cluster::forward(p, cmd);
        }
        // Reads served locally
        return engine.exec(cmd);
    }

    // Primary / standalone: execute locally.
    std::string result;

    if (!cluster.enabled()) {
        result = engine.exec(cmd);
    } else {
        std::string pk = engine.pk_bytes_for_routing(cmd);
        if (!pk.empty()) {
            const PeerInfo* peer = cluster.route_key(pk);
            result = peer ? Cluster::forward(*peer, cmd) : engine.exec(cmd);
        } else {
            std::string lo, hi;
            if (engine.range_bytes_for_routing(cmd, lo, hi)) {
                result = engine.exec(cmd);
                for (const PeerInfo* p : cluster.route_range(lo, hi))
                    result = merge_range_results(result, Cluster::forward(*p, cmd));
            } else {
                result = engine.exec(cmd);
            }
        }
    }

    // Primary: replicate successful mutations.
    if (repl_log && should_replicate(cmd, result))
        repl_log->append(cmd);

    return result;
}

// ---- connection handler ----

static void handle_conn(int conn_fd, Engine& engine, const Cluster& cluster,
                         ReplLog* repl_log, const std::string& primary_addr) {
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
            result = route_and_exec(cmd, engine, cluster, repl_log, primary_addr);
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
    int         port         = 5432;
    std::string data_dir     = "./data";
    std::string cluster_file;
    std::string primary_addr;
    int         repl_port    = 0;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if      (a == "--port"      && i + 1 < argc) { port         = std::stoi(argv[++i]); }
        else if (a == "--data"      && i + 1 < argc) { data_dir     = argv[++i]; }
        else if (a == "--cluster"   && i + 1 < argc) { cluster_file = argv[++i]; }
        else if (a == "--primary"   && i + 1 < argc) { primary_addr = argv[++i]; }
        else if (a == "--repl-port" && i + 1 < argc) { repl_port    = std::stoi(argv[++i]); }
        else { data_dir = a; }
    }

    std::filesystem::create_directories(data_dir);
    std::cerr << "heavy-trie server  port=" << port
              << "  data=" << data_dir << "\n";

    Cluster cluster;
    if (!cluster_file.empty()) {
        cluster = Cluster::load(cluster_file, port);
        std::cerr << "cluster config: " << cluster_file << "\n";
    }

    Engine engine(data_dir);

    // ---- replication setup ----
    std::unique_ptr<ReplLog> repl_log;
    std::unique_ptr<ReplClient> repl_client;
    std::vector<std::thread> repl_feed_threads;
    int repl_srv_fd = -1;

    if (!primary_addr.empty()) {
        // Replica mode: connect to primary and stream commands.
        repl_client = std::make_unique<ReplClient>();
        repl_client->start(
            primary_addr.substr(0, primary_addr.rfind(':')),
            std::stoi(primary_addr.substr(primary_addr.rfind(':') + 1)),
            [&engine](const std::string& cmd) { engine.exec(cmd); },
            data_dir + "/repl_lsn"
        );
        std::cerr << "replica mode: primary=" << primary_addr << "\n";
    } else if (repl_port > 0) {
        // Primary mode: open replication log and listen for replica connections.
        repl_log = std::make_unique<ReplLog>(ReplLog::open(data_dir + "/repl.log"));
        std::cerr << "primary mode: repl-port=" << repl_port
                  << "  next-lsn=" << repl_log->next_lsn() << "\n";

        repl_srv_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int ropt = 1;
        ::setsockopt(repl_srv_fd, SOL_SOCKET, SO_REUSEADDR, &ropt, sizeof(ropt));
        sockaddr_in ra{};
        ra.sin_family      = AF_INET;
        ra.sin_addr.s_addr = INADDR_ANY;
        ra.sin_port        = htons(static_cast<uint16_t>(repl_port));
        if (::bind(repl_srv_fd, reinterpret_cast<sockaddr*>(&ra), sizeof(ra)) < 0)
            { perror("repl bind"); return 1; }
        ::listen(repl_srv_fd, 16);

        // Background thread accepts replica connections.
        repl_feed_threads.emplace_back([repl_srv_fd_cap = repl_srv_fd,
                                        &repl_log, &repl_feed_threads]() {
            while (!g_stop) {
                fd_set rfds; FD_ZERO(&rfds); FD_SET(repl_srv_fd_cap, &rfds);
                timeval tv{1, 0};
                if (::select(repl_srv_fd_cap + 1, &rfds, nullptr, nullptr, &tv) <= 0)
                    continue;
                int rfd = ::accept(repl_srv_fd_cap, nullptr, nullptr);
                if (rfd < 0) continue;

                // Read SYNC <lsn> handshake
                std::string msg;
                if (!recv_msg(rfd, msg) || msg.substr(0, 5) != "SYNC ") {
                    ::close(rfd); continue;
                }
                uint64_t start_lsn = std::stoull(msg.substr(5));
                std::cerr << "[repl] replica connected, streaming from LSN "
                          << start_lsn << "\n";

                repl_feed_threads.emplace_back([rfd, start_lsn, &repl_log]() {
                    repl_log->feed_replica(rfd, start_lsn, g_stop);
                    std::cerr << "[repl] replica disconnected\n";
                });
            }
        });
    }

    // ---- signal handlers ----
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

        workers.emplace_back([conn_fd, &engine, &cluster,
                               &repl_log, &primary_addr]() {
            handle_conn(conn_fd, engine, cluster, repl_log.get(), primary_addr);
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

    if (repl_client) repl_client->stop();
    if (repl_srv_fd >= 0) ::close(repl_srv_fd);
    for (auto& th : repl_feed_threads)
        if (th.joinable()) th.detach();

    std::cerr << "server stopped\n";
    return 0;
}
