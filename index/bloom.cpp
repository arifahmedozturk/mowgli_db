#include "index/bloom.h"
#include <algorithm>
#include <cstdio>

static uint64_t fmix64(uint64_t h) {
    h ^= h >> 33; h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33; h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

static uint64_t bloom_hash(const std::string& key) {
    uint64_t h = 14695981039346656037ULL;   // FNV-1a, then a full avalanche
    for (unsigned char c : key) { h ^= c; h *= 1099511628211ULL; }
    return fmix64(h);
}

static constexpr size_t BLOOM_BLOCK_WORDS = 8;   // 64 bytes = 512 bits

// The block a key maps to in a stage of nblocks blocks, and its bits in it:
// K 9-bit positions taken from a second mix of the hash.
static size_t bloom_block(uint64_t h, size_t nblocks) {
    return static_cast<size_t>((static_cast<unsigned __int128>(h) * nblocks) >> 64);
}

void BloomFilter::add_stage(uint64_t capacity) {
    uint64_t blocks = (capacity * BITS_PER_KEY + 511) / 512;
    Stage st;
    st.capacity = capacity;
    st.words.assign(blocks * BLOOM_BLOCK_WORDS, 0);
    stages_.push_back(std::move(st));
}

void BloomFilter::reserve(uint64_t expected_keys) {
    if (stages_.empty()) add_stage(std::max<uint64_t>(MIN_KEYS, 2 * expected_keys));
}

void BloomFilter::add(const std::string& key) {
    if (stages_.empty()) add_stage(MIN_KEYS);
    if (stages_.back().count >= stages_.back().capacity) add_stage(stages_.back().capacity * GROWTH);
    Stage& st = stages_.back();
    uint64_t h = bloom_hash(key), g = fmix64(h ^ 0x9E3779B97F4A7C15ULL);
    uint64_t* blk = st.words.data() + bloom_block(h, st.words.size() / BLOOM_BLOCK_WORDS) * BLOOM_BLOCK_WORDS;
    for (int i = 0; i < K; i++) {
        unsigned pos = (g >> (9 * i)) & 511;
        blk[pos >> 6] |= 1ULL << (pos & 63);
    }
    st.count++;
}

static constexpr uint32_t BLOOM_FILE_MAGIC = 0x424C4D32; // "BLM2"

bool BloomFilter::load(const std::string& path, uint64_t key_count) {
    FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) return false;
    uint32_t magic = 0, stages = 0;
    uint64_t keys  = 0;
    bool ok = std::fread(&magic, 4, 1, f) == 1 && magic == BLOOM_FILE_MAGIC
           && std::fread(&stages, 4, 1, f) == 1 && stages <= 64
           && std::fread(&keys,  8, 1, f) == 1 && keys == key_count;
    std::vector<Stage> loaded;
    for (uint32_t i = 0; ok && i < stages; i++) {
        Stage st;
        uint64_t words = 0;
        ok = std::fread(&st.capacity, 8, 1, f) == 1 && std::fread(&st.count, 8, 1, f) == 1
          && std::fread(&words, 8, 1, f) == 1 && words % BLOOM_BLOCK_WORDS == 0 && words > 0
          && words == (st.capacity * BITS_PER_KEY + 511) / 512 * BLOOM_BLOCK_WORDS;
        if (!ok) break;
        st.words.resize(words);
        ok = std::fread(st.words.data(), 8, words, f) == words;
        loaded.push_back(std::move(st));
    }
    std::fclose(f);
    if (ok) stages_ = std::move(loaded);
    return ok;
}

void BloomFilter::save(const std::string& path, uint64_t key_count) const {
    if (stages_.empty()) return;
    std::string tmp = path + ".tmp";
    FILE* f = std::fopen(tmp.c_str(), "wb");
    if (!f) return;  // best effort: the next open rebuilds
    uint32_t magic = BLOOM_FILE_MAGIC, stages = static_cast<uint32_t>(stages_.size());
    uint64_t keys  = key_count;
    bool ok = std::fwrite(&magic, 4, 1, f) == 1 && std::fwrite(&stages, 4, 1, f) == 1
           && std::fwrite(&keys, 8, 1, f) == 1;
    for (const auto& st : stages_) {
        uint64_t words = st.words.size();
        ok = ok && std::fwrite(&st.capacity, 8, 1, f) == 1 && std::fwrite(&st.count, 8, 1, f) == 1
                && std::fwrite(&words, 8, 1, f) == 1
                && std::fwrite(st.words.data(), 8, words, f) == words;
    }
    ok = std::fclose(f) == 0 && ok;
    if (ok) std::rename(tmp.c_str(), path.c_str());
    else    std::remove(tmp.c_str());
}

bool BloomFilter::may_contain(const std::string& key) const {
    if (stages_.empty()) return true; // not yet initialised — assume present
    uint64_t h = bloom_hash(key), g = fmix64(h ^ 0x9E3779B97F4A7C15ULL);
    // Each stage's block is a likely cache miss once the filter outgrows the
    // CPU cache: fetch them all at once so the misses overlap.
    const uint64_t* blocks[64];
    for (size_t s = 0; s < stages_.size(); s++) {
        const Stage& st = stages_[s];
        blocks[s] = st.words.data() + bloom_block(h, st.words.size() / BLOOM_BLOCK_WORDS) * BLOOM_BLOCK_WORDS;
        __builtin_prefetch(blocks[s]);
    }
    for (size_t s = stages_.size(); s-- > 0;) {
        const uint64_t* blk = blocks[s];
        bool all = true;
        for (int i = 0; i < K && all; i++) {
            unsigned pos = (g >> (9 * i)) & 511;
            all = (blk[pos >> 6] >> (pos & 63)) & 1;
        }
        if (all) return true;
    }
    return false;
}
