#pragma once
#include <cstdint>
#include <string>
#include <vector>

// Scalable blocked Bloom filter (#76, #22) for the trie's negative lookups.
// A key's K bits all lie in one 64-byte block of a stage, so probing a stage
// is one cache miss. Each stage holds up to `capacity` keys at BITS_PER_KEY
// bits per key (~1% false positives); when the newest is full a stage GROWTH
// times larger is added, so the filter grows with the table without
// re-reading its keys. Lookups prefetch every stage's block, then probe the
// newest (largest) stage first.
class BloomFilter {
public:
    static constexpr int      K            = 7;
    static constexpr uint64_t BITS_PER_KEY = 10;
    static constexpr uint64_t MIN_KEYS     = 1u << 18;   // first stage: 256K keys, 320 KB
    static constexpr uint64_t GROWTH       = 4;

    struct Stage {
        uint64_t capacity = 0, count = 0;
        std::vector<uint64_t> words;   // 8 words (64 bytes) per block
    };

    bool empty() const { return stages_.empty(); }
    void clear() { stages_.clear(); }
    // If there is no stage yet, start one for about twice expected_keys (at
    // least MIN_KEYS), so a known key count fits without growing.
    void reserve(uint64_t expected_keys);
    void add(const std::string& key);
    // false = definitely absent; true when the filter is empty.
    bool may_contain(const std::string& key) const;
    const std::vector<Stage>& stages() const { return stages_; }

    // Sidecar file: [magic u32][stages u32][key_count u64], then per stage
    // [capacity u64][count u64][words u64][words...]. load() rejects a file
    // whose key_count differs (one saved before later writes).
    bool load(const std::string& path, uint64_t key_count);
    void save(const std::string& path, uint64_t key_count) const;   // temp file + rename

private:
    void add_stage(uint64_t capacity);
    std::vector<Stage> stages_;
};
