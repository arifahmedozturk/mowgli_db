#pragma once
#include "index/chain.h"
#include "storage/buffer_pool.h"
#include <array>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

// ---- chain address helpers ----
// Top 8 bits of a chain address = packed slot index (0 = dedicated block).
// Bottom 56 bits = physical block ID.
// NULL_BLOCK (UINT64_MAX) remains the null sentinel.
inline uint8_t  chain_addr_slot(uint64_t addr) { return static_cast<uint8_t>(addr >> 56); }
inline uint64_t chain_addr_phys(uint64_t addr) { return addr & CHAIN_PHYS_MASK; }
inline uint64_t make_chain_addr(uint64_t phys, uint8_t slot) {
    return (static_cast<uint64_t>(slot) << 56) | phys;
}

// Chains whose encoded size fits within PACK_THRESHOLD bytes are stored in
// packed blocks (multiple chains per 8 KB block).  Larger chains get their own
// dedicated block (slot == 0), identical to the old behaviour.
static constexpr size_t PACK_THRESHOLD = 256;

// In-memory state for one packed physical block.
struct PackedBlockState {
    uint8_t  num_slots  = 0;
    uint16_t data_end   = 0;   // bytes used in data area (relative to PACK_DATA_OFFSET)
    uint8_t  live_count = 0;   // slots that hold a live (non-forwarded) chain
    std::array<uint16_t, PACK_MAX_SLOTS> offsets{};  // absolute byte offset in block
    std::array<uint16_t, PACK_MAX_SLOTS> lengths{};  // byte length (0 = dead/empty)
};

// File layout:
//   block 0        — file header (magic, root block, key count, next free block)
//   block 1..N     — chain blocks

class DiskManager {
public:
    // Create a new file (overwrites if exists).
    // alloc_batch: how many block IDs to pre-commit per header flush (default 64).
    static DiskManager create(const std::string& path, uint64_t alloc_batch = 64);
    // Open an existing file.
    static DiskManager open(const std::string& path, uint64_t alloc_batch = 64);

    ~DiskManager();
    DiskManager(DiskManager&& o) noexcept
        : fd_(o.fd_), root_block_(o.root_block_),
          key_count_(o.key_count_), next_free_block_(o.next_free_block_),
          committed_ceil_(o.committed_ceil_), alloc_batch_(o.alloc_batch_),
          free_list_mem_(std::move(o.free_list_mem_))
    { o.fd_ = -1; }
    DiskManager& operator=(DiskManager&&)      = delete;
    DiskManager(const DiskManager&)            = delete;
    DiskManager& operator=(const DiskManager&) = delete;

    // Allocate a fresh block, returns its block id.
    // Reuses a recycled block from the free list if available.
    uint64_t alloc_block();

    // Return a block to the free list so it can be reused by future alloc_block calls.
    void free_block(uint64_t block_id);

    void read_block (uint64_t block_id, uint8_t out[BLOCK_SIZE]) const;
    void write_block(uint64_t block_id, const uint8_t buf[BLOCK_SIZE]);

    // Encode chain, allocate a block, write it. Returns the new block id.
    // (Legacy: always allocates a dedicated block.)
    uint64_t write_chain(const ChainData& chain);
    // Encode chain and overwrite an existing chain address.
    // Delegates to update_chain_at; ignores address change from promotions.
    void update_chain(uint64_t chain_addr, const ChainData& chain);
    // Read the chain at chain_addr (dedicated or packed, follows forwarding stubs).
    ChainData read_chain(uint64_t chain_addr) const;

    // ---- packed-block allocation API ----

    // Allocate a slot for a chain.  Chains whose encoded size fits within
    // PACK_THRESHOLD go into a shared packed block; larger chains get a
    // dedicated block.  Returns a chain address (slot bits may be non-zero).
    uint64_t alloc_chain_slot(const ChainData& chain, uint64_t hint_phys = NULL_BLOCK);

    // Free a chain slot.  For packed chains, marks the slot dead and optionally
    // recycles the physical block.  Follows forwarding stubs when present so
    // the promoted dedicated block is also freed.
    void free_chain_slot(uint64_t chain_addr);

    // Read a chain at chain_addr: handles dedicated, packed, and forwarded slots.
    ChainData read_chain_at(uint64_t chain_addr) const;

    // Write a chain at chain_addr.  For packed slots whose new encoded size
    // exceeds their original capacity, promotes to a dedicated block and writes
    // a forwarding stub to the old slot.  Returns the (possibly new) chain
    // address; callers may use it as a hint but need not update parent pointers
    // immediately — the forwarding stub keeps the old address valid.
    uint64_t update_chain_at(uint64_t chain_addr, const ChainData& chain);

    uint64_t root_block() const { std::lock_guard<std::mutex> l(header_mutex_); return root_block_; }
    void     set_root_block(uint64_t id);

    uint64_t key_count() const { std::lock_guard<std::mutex> l(header_mutex_); return key_count_; }
    void     set_key_count(uint64_t n);

    // Number of chain blocks allocated (block 0 is the header, not a chain).
    uint64_t chain_count() const { std::lock_guard<std::mutex> l(header_mutex_); return next_free_block_ > 1 ? next_free_block_ - 1 : 0; }

    // Latch-coupled access for DiskTrie traversal.
    // Caller must call the matching unpin exactly once per pin.
    const uint8_t* pin_chain_shared   (uint64_t block_id) const;
    void           unpin_chain_shared (uint64_t block_id) const;
    uint8_t*       pin_chain_exclusive(uint64_t block_id);
    void           unpin_chain_exclusive(uint64_t block_id);

    // Clear the list of candidate packed blocks so the next alloc_chain_slot
    // calls start fresh (used by DiskTrie::compact to avoid mixing new and old data).
    void clear_pack_candidates() {
        std::lock_guard<std::mutex> lp(pack_mutex_);
        pack_candidates_.clear();
    }

    BufferPool& pool() { return pool_; }

private:
    // Default pool size: 256 blocks = 2MB. Tune as needed.
    static constexpr size_t DEFAULT_POOL_CAPACITY = 256;
    explicit DiskManager(int fd, uint64_t alloc_batch = 64);
    void read_header();
    void flush_header();
    // Scan all allocated blocks on open; rebuild packed_blocks_ and pack_candidates_.
    void rebuild_packed_state();
    void rebuild_packed_block(uint64_t phys);

    int                fd_;
    mutable BufferPool pool_{DEFAULT_POOL_CAPACITY};
    mutable std::mutex header_mutex_; // protects root/key_count/next_free/committed/free_list
    uint64_t           root_block_        = NULL_BLOCK;
    uint64_t           key_count_         = 0;
    uint64_t           next_free_block_   = 1;
    uint64_t           committed_ceil_    = 1;
    uint64_t           alloc_batch_       = 64;
    std::vector<uint64_t> free_list_mem_;

    // Lock order: pack_mutex_ acquired before (or independently of) header_mutex_;
    // stripe latches always acquired last.
    mutable std::mutex pack_mutex_;   // protects packed_blocks_ and pack_candidates_
    std::unordered_map<uint64_t, PackedBlockState> packed_blocks_;
    std::vector<uint64_t> pack_candidates_; // phys blocks that still have room
};
