#include "server/cluster.h"
#include "server/wire.h"
#include <algorithm>
#include <fstream>
#include <netdb.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

static std::string hex_decode(const std::string& s) {
    if (s == "-") return {};
    if (s.size() % 2 != 0)
        throw std::runtime_error("invalid hex range boundary: " + s);
    std::string out(s.size() / 2, '\0');
    for (size_t k = 0; k < s.size(); k += 2) {
        auto nib = [&](char c) -> uint8_t {
            if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
            if (c >= 'a' && c <= 'f') return static_cast<uint8_t>(c - 'a' + 10);
            throw std::runtime_error(std::string("invalid hex char: ") + c);
        };
        out[k / 2] = static_cast<char>((nib(s[k]) << 4) | nib(s[k + 1]));
    }
    return out;
}

bool PeerInfo::owns(const std::string& key) const {
    if (!range_lo.empty() && key < range_lo) return false;
    if (!range_hi.empty() && key >= range_hi) return false;
    return true;
}

// Query range [lo, hi] (inclusive) vs peer range [range_lo, range_hi) (hi exclusive).
bool PeerInfo::overlaps(const std::string& lo, const std::string& hi) const {
    if (!range_hi.empty() && lo >= range_hi) return false;
    if (!range_lo.empty() && hi < range_lo) return false;
    return true;
}

Cluster Cluster::load(const std::string& path, int self_port) {
    Cluster c;
    c.self_port_ = self_port;
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open cluster config: " + path);
    std::string host, lo_hex, hi_hex;
    int port;
    while (f >> host >> port >> lo_hex >> hi_hex) {
        PeerInfo p;
        p.host     = host;
        p.port     = port;
        p.range_lo = hex_decode(lo_hex);
        p.range_hi = hex_decode(hi_hex);
        c.peers_.push_back(std::move(p));
    }
    return c;
}

const PeerInfo* Cluster::route_key(const std::string& key) const {
    for (const auto& p : peers_) {
        if (p.port == self_port_) continue;
        if (p.owns(key)) return &p;
    }
    return nullptr;
}

std::vector<const PeerInfo*> Cluster::route_range(const std::string& lo,
                                                    const std::string& hi) const {
    std::vector<const PeerInfo*> result;
    for (const auto& p : peers_) {
        if (p.port == self_port_) continue;
        if (p.overlaps(lo, hi)) result.push_back(&p);
    }
    std::sort(result.begin(), result.end(),
              [](const PeerInfo* a, const PeerInfo* b) {
                  return a->range_lo < b->range_lo;
              });
    return result;
}

std::string Cluster::forward(const PeerInfo& peer, const std::string& cmd) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket() failed");

    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    std::string port_str = std::to_string(peer.port);
    if (::getaddrinfo(peer.host.c_str(), port_str.c_str(), &hints, &res) != 0 || !res) {
        ::close(fd);
        throw std::runtime_error("cannot resolve " + peer.host);
    }

    bool ok = ::connect(fd, res->ai_addr, res->ai_addrlen) == 0;
    ::freeaddrinfo(res);
    if (!ok) {
        ::close(fd);
        throw std::runtime_error("cannot connect to " + peer.host + ":" + port_str);
    }

    if (!send_msg(fd, cmd)) {
        ::close(fd);
        throw std::runtime_error("send_msg to peer failed");
    }
    std::string response;
    if (!recv_msg(fd, response)) {
        ::close(fd);
        throw std::runtime_error("recv_msg from peer failed");
    }
    ::close(fd);
    return response;
}
