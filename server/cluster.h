#pragma once
#include <string>
#include <vector>

struct PeerInfo {
    std::string host;
    int         port     = 0;
    std::string range_lo; // inclusive, raw bytes, empty = absolute min
    std::string range_hi; // exclusive, raw bytes, empty = absolute max

    bool owns(const std::string& key_bytes) const;
    // True if [lo, hi] (inclusive both ends) overlaps this peer's range.
    bool overlaps(const std::string& lo, const std::string& hi) const;
};

class Cluster {
public:
    Cluster() = default;

    // Config file format: one peer per line
    //   host port lo_hex hi_hex
    // lo_hex / hi_hex: lowercase hex byte pairs, or "-" for open end.
    // Example:
    //   127.0.0.1 5432 - 0000000080000000
    //   127.0.0.1 5433 0000000080000000 -
    static Cluster load(const std::string& path, int self_port);

    bool enabled() const { return !peers_.empty(); }

    // Returns nullptr if key_bytes falls in this node's range.
    const PeerInfo* route_key(const std::string& key_bytes) const;

    // Returns remote peers (excluding self) whose range overlaps [lo, hi],
    // sorted by range_lo so callers can concatenate RANGE results in order.
    std::vector<const PeerInfo*> route_range(const std::string& lo,
                                              const std::string& hi) const;

    // Open a connection to peer, send cmd, return the response.
    static std::string forward(const PeerInfo& peer, const std::string& cmd);

private:
    std::vector<PeerInfo> peers_;
    int                   self_port_ = 0;
};
