#pragma once
#include <cstdint>
#include <string>
#include <vector>

static constexpr uint32_t BLOCK_SIZE = 8192;
static constexpr uint64_t NULL_BLOCK = UINT64_MAX;

// ---- packed block constants ----
// A packed block stores multiple small chains in one 8 KB block.
// Layout: [magic(4)] [num_slots(2)] [dir[255]: {offset(2), len(2)}] [chain data...]
// The directory is a fixed 255-entry × 4-byte table = 1020 bytes.
// Chain data begins at byte PACK_DATA_OFFSET = 1026.
static constexpr uint32_t PACK_BLOCK_MAGIC  = 0xABC4AB47;
static constexpr uint16_t FORWARD_MAGIC     = 0xFFFE; // forwarding-stub tag
static constexpr size_t   PACK_MAX_SLOTS    = 255;    // max chains per packed block
static constexpr size_t   PACK_DIR_OFFSET   = 6;      // after magic(4)+num_slots(2)
static constexpr size_t   PACK_DATA_OFFSET  = PACK_DIR_OFFSET + PACK_MAX_SLOTS * 4; // 1026
static constexpr size_t   PACK_DATA_SIZE    = BLOCK_SIZE - PACK_DATA_OFFSET;        // 7166

// Chain address encoding: top 8 bits = slot (0 = dedicated, 1..255 = packed slot),
// bottom 56 bits = physical block ID.
// NULL_BLOCK (UINT64_MAX) is still the null sentinel.
static constexpr uint64_t CHAIN_PHYS_MASK   = (1ULL << 56) - 1;

// Pointer to a record stored in the heap file.
// block_id == NULL_BLOCK means "no record" (non-terminal node).
struct RecordPtr {
    uint64_t block_id = NULL_BLOCK;
    uint16_t slot     = 0;
    bool valid() const { return block_id != NULL_BLOCK; }
};

// ---- base-16 (nibble) trie ----
// Keys are walked 4 bits at a time: a key of n bytes is 2n nibbles, high
// nibble first. Each branching node has one heavy continuation (the chain's
// own next nibble) and up to 15 light children, one per other nibble value.
static constexpr unsigned NIBBLE_BITS  = 4;
static constexpr unsigned NIBBLE_FANOUT = 16;

inline uint8_t key_nibble(const std::string& key, size_t pos) {
    uint8_t byte = static_cast<uint8_t>(key[pos / 2]);
    return (pos % 2 == 0) ? (byte >> 4) : (byte & 0x0F);
}

// A light child hanging off a chain node, selected by nibble `digit`.
struct LightChild {
    uint8_t  digit  = 0;           // 0..15, never the heavy digit at this node
    uint16_t weight = 0;           // floor(log2(subtree key count)), serialized
    uint64_t block  = NULL_BLOCK;  // chain address of the light subtree
};

// Logical (in-memory) representation of one chain block.
struct ChainNode {
    uint16_t  split = 0;               // nibble index in the chain's path
    uint16_t  heavy_weight = 0;        // floor(log2(heavy-side key count)), serialized
    std::vector<LightChild> lights;    // sorted by digit
    RecordPtr record;                  // valid() == true means a key ends here

    // Index into lights of the child with this digit, or -1.
    int light_index(uint8_t digit) const {
        for (size_t i = 0; i < lights.size(); i++)
            if (lights[i].digit == digit) return static_cast<int>(i);
        return -1;
    }
};

struct ChainData {
    std::vector<uint8_t>   path;       // one nibble (0..15) per element
    uint16_t               path_len = 0;
    uint8_t                phase    = 0; // absolute start nibble % 2 (keys end at even depths)
    std::vector<ChainNode> nodes;      // branch points, ordered by split
    RecordPtr              tail_record; // valid() == true means a key ends at chain leaf
};

// A key can end at chain position pos only on a byte boundary.
inline bool node_can_hold_record(uint8_t phase, size_t pos) { return (phase + pos) % 2 == 0; }

bool   chain_encode(const ChainData& chain, uint8_t out[BLOCK_SIZE]);
bool   chain_decode(const uint8_t buf[BLOCK_SIZE], ChainData& out);

// Encode a chain into an arbitrary byte buffer (for packed block storage).
// Returns bytes written, or 0 if the buffer is too small.
size_t chain_encode_slice(const ChainData& chain, uint8_t* out, size_t max_size);
// Decode a chain from an arbitrary byte slice.
bool   chain_decode_slice(const uint8_t* buf, size_t len, ChainData& out);

// Result of matching a key against a chain's path.
struct MatchOutcome {
    enum class Kind { FOUND, MISS, FOLLOW_LIGHT } kind;
    RecordPtr record;               // valid when FOUND
    size_t    next_key_pos = 0;     // FOLLOW_LIGHT only: key nibble where the child starts
    size_t    node_index   = 0;     // FOLLOW_LIGHT only
    uint64_t  light_child  = NULL_BLOCK; // FOLLOW_LIGHT only
};

// key_pos: nibble of the key where this chain starts.
MatchOutcome chain_match(const ChainData& chain,
                         const std::string& key,
                         size_t key_pos);

// Same as chain_match, but on an encoded chain (chain_encode / chain_encode_slice
// bytes) without decoding it into a ChainData: no allocation per hop.
// Sets ok = false if the bytes are not a valid chain.
MatchOutcome chain_match_raw(const uint8_t* buf, size_t len,
                             const std::string& key, size_t key_pos, bool& ok);
