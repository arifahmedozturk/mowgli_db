#pragma once
#include <cstdint>
#include <string>
#include <vector>

// Compressed-chain magic. First 4 bytes of a chain block that has been
// compressed with delta-encoding + zstd. Distinct from CHAIN_MAGIC so
// old uncompressed files continue to open without changes.
static constexpr uint32_t COMPRESS_MAGIC = 0x5A535444; // "ZSTD"

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

// Logical (in-memory) representation of one chain block.
struct ChainNode {
    uint8_t   split_bit;
    uint16_t  light_child_weight;  // floor(log2(light_count)), serialized
    uint16_t  heavy_child_weight;  // floor(log2(heavy_count)), serialized
    uint64_t  light_child_block;   // NULL_BLOCK if none
    RecordPtr record;              // valid() == true means a key ends here
};

struct ChainData {
    std::vector<uint8_t>  path_bits;   // packed, ceil(path_bit_len/8) bytes
    uint16_t              path_bit_len = 0;
    uint8_t               bit_phase    = 0; // (absolute_start_bit % 8) for this chain
    std::vector<ChainNode> nodes;      // branch points, ordered by split_bit
    RecordPtr             tail_record; // valid() == true means a key ends at chain leaf
};

bool   chain_encode(const ChainData& chain, uint8_t out[BLOCK_SIZE]);
bool   chain_decode(const uint8_t buf[BLOCK_SIZE], ChainData& out);

// Compress raw chain bytes (output of chain_encode_slice) with delta-encoding
// on the uint64 block-address fields, then zstd level 1.
// Returns compressed size written into |out|, or 0 if compression expanded the data
// (caller should store the raw bytes instead).
// |out| must have capacity >= BLOCK_SIZE.
size_t chain_compress(const uint8_t* raw, size_t raw_len,
                      uint8_t* out,       size_t out_cap);

// Decompress bytes produced by chain_compress into |out|.
// Returns the original (decompressed) byte count, or 0 on error.
// Safe to call on raw (uncompressed) bytes — returns 0 so caller falls back.
size_t chain_decompress(const uint8_t* in,  size_t in_len,
                        uint8_t* out,        size_t out_cap);

// Encode a chain into an arbitrary byte buffer (for packed block storage).
// Returns bytes written, or 0 if the buffer is too small.
size_t chain_encode_slice(const ChainData& chain, uint8_t* out, size_t max_size);
// Decode a chain from an arbitrary byte slice.
bool   chain_decode_slice(const uint8_t* buf, size_t len, ChainData& out);

// Result of matching a key against a chain's path.
struct MatchOutcome {
    enum class Kind { FOUND, MISS, FOLLOW_LIGHT } kind;
    RecordPtr record;           // valid when FOUND
    size_t    next_key_bit = 0; // FOLLOW_LIGHT only
    size_t    node_index   = 0; // FOLLOW_LIGHT only
};

MatchOutcome chain_match(const ChainData& chain,
                         const std::string& key,
                         size_t key_bit_start);
