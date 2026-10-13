#pragma once
#include "index/chain.h"
#include "storage/buffer_pool.h"
#include <array>
#include <cstdint>
#include <memory>
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

// Chains and buckets up to PACK_THRESHOLD bytes are stored in packed blocks
// (many per 8 KB block), in a slot sized to the next power of two between
// PACK_MIN_SLOT and PACK_THRESHOLD (#18); the rounding leaves room to grow in
// place. Larger ones get their own dedicated block (slot == 0).
static constexpr size_t PACK_MIN_SLOT  = 64;
static constexpr size_t PACK_THRESHOLD = 4096;

// Slot capacity for an encoded size of n bytes (n <= PACK_THRESHOLD).
inline size_t pack_slot_class(size_t n) {
    size_t c = PACK_MIN_SLOT;
    while (c < n) c <<= 1;
    return c;
}

// In-memory state for one packed physical block.
// Slot regions are laid out in slot order and never move; a freed slot keeps
// its offset (length 0 = dead), so its region can be reused (#19).
struct PackedBlockState {
    uint8_t  num_slots  = 0;
    uint16_t data_end   = 0;   // bytes used in data area (relative to PACK_DATA_OFFSET)
    uint8_t  live_count = 0;   // slots that hold a live (non-forwarded) chain
    bool     in_candidates = false;
    std::array<uint16_t, PACK_MAX_SLOTS> offsets{};  // absolute byte offset in block
    std::array<uint16_t, PACK_MAX_SLOTS> lengths{};  // capacity in use (0 = dead/empty)

    // Size of slot i's region: up to the next slot, or the end of used data.
    uint16_t capacity(size_t i) const {
        if (offsets[i] == 0) return 0;
        uint16_t end = (i + 1 < num_slots) ? offsets[i + 1]
                                           : static_cast<uint16_t>(PACK_DATA_OFFSET + data_end);
        return end > offsets[i] ? static_cast<uint16_t>(end - offsets[i]) : 0;
    }
    // The smallest dead slot whose region holds need bytes, or -1.
    int dead_fit(size_t need) const {
        int best = -1;
        for (size_t i = 0; i < num_slots; i++)
            if (lengths[i] == 0 && capacity(i) >= need &&
                (best < 0 || capacity(i) < capacity(static_cast<size_t>(best))))
                best = static_cast<int>(i);
        return best;
    }
    // Room for need bytes: in a dead slot, or at the tail.
    bool fits(size_t need) const {
        return dead_fit(need) >= 0 ||
               (num_slots < PACK_MAX_SLOTS && PACK_DATA_SIZE - data_end >= need);
    }
};

// File layout:
//   block 0        — file header (magic, root block, key count, next free block)
//   block 1..N     — chain blocks

class DiskManager {
public:
    // Create a new file (overwrites if exists).
    // alloc_batch: how many block IDs to pre-commit per header flush (default 64).
    static std::unique_ptr<DiskManager> create(const std::string& path,
                                               uint64_t alloc_batch = 64);
    // Open an existing file.
    static std::unique_ptr<DiskManager> open(const std::string& path,
                                             uint64_t alloc_batch = 64);

    ~DiskManager();
    DiskManager(DiskManager&&)            = delete;
    DiskManager& operator=(DiskManager&&) = delete;
    DiskManager(const DiskManager&)       = delete;
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
    // PACK_THRESHOLD go into a size-classed slot of a shared packed block;
    // larger chains get a dedicated block.  Returns a chain address (slot bits
    // may be non-zero).
    uint64_t alloc_chain_slot(const ChainData& chain, uint64_t hint_phys = NULL_BLOCK);

    // Free a chain slot.  For packed chains, marks the slot dead and optionally
    // recycles the physical block.  Follows a forwarding stub when present so
    // the chain's current location is also freed.
    void free_chain_slot(uint64_t chain_addr);

    // Read a chain at chain_addr: handles dedicated, packed, and forwarded slots.
    ChainData read_chain_at(uint64_t chain_addr) const;

    // Write a chain at chain_addr.  A packed chain that outgrows its slot moves
    // to a larger slot class (or a dedicated block past PACK_THRESHOLD) and the
    // old slot becomes a forwarding stub.  A stub always points straight at the
    // data: if forwarded data moves again, the stub is repointed and the old
    // copy freed.  Returns the chain's current location; callers may use it as
    // a hint but need not update parent pointers — the stub keeps
    // chain_addr valid.
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

    // Pin the encoded bytes of the chain at chain_addr (dedicated block, packed
    // slot, or the block a forwarding stub points to) for zero-copy reading.
    // Release with unpin_chain_shared(pinned_block).
    struct ChainBytes { const uint8_t* data; size_t len; uint64_t pinned_block; };
    ChainBytes     pin_chain_bytes    (uint64_t chain_addr) const;
    // Writable access to the encoded bytes at chain_addr where they live
    // (following a forwarding stub), for edits in place (#73). capacity is
    // what the slot can hold: its size class, or a whole dedicated block.
    // Release with unpin_chain_exclusive(pinned_block).
    struct ChainSlot { uint8_t* data; size_t capacity; uint64_t pinned_block; };
    ChainSlot      pin_chain_slot     (uint64_t chain_addr);
    // True if chain_addr holds a forwarding stub (for tests).
    bool           is_forwarded       (uint64_t chain_addr) const;
    uint8_t*       pin_chain_exclusive(uint64_t block_id);
    void           unpin_chain_exclusive(uint64_t block_id);

    // Clear the list of candidate packed blocks so the next alloc_chain_slot
    // calls start fresh (used by DiskTrie::compact to avoid mixing new and old data).
    void clear_pack_candidates() {
        std::lock_guard<std::mutex> lp(pack_mutex_);
        pack_candidates_.clear();
    }

    // Slide the given blocks (sorted ascending; every other chain block is
    // garbage) down to ids 1..n in the same order, re-key the packed-block
    // state, drop the free list and truncate the file after them. Chain
    // pointers must already name the new ids (see compacted_phys). Not crash
    // safe: blocks are overwritten in place.
    void compact_blocks(const std::vector<uint64_t>& live);
    // The id the block with rank i (0-based) in compact_blocks' list gets.
    static uint64_t compacted_phys(size_t rank) { return rank + 1; }

    BufferPool& pool() { return pool_; }

private:
    // Store already-encoded chain bytes in a new location (packed slot of the
    // right class, or a dedicated block). Returns its address.
    uint64_t place_bytes(const uint8_t* bytes, size_t len, uint64_t hint_phys);
    // Free one packed slot without following a forwarding stub; returns the
    // stub's target if the slot held one (NULL_BLOCK otherwise).
    uint64_t free_packed_slot(uint64_t chain_addr);
    // Default pool size: 256 blocks = 2MB. Tune as needed.
    static constexpr size_t DEFAULT_POOL_CAPACITY = 256;
    explicit DiskManager(int fd, uint64_t alloc_batch = 64);
    void read_header();
    // spill=true (on close) also writes free IDs that don't fit in the header
    // to overflow pages; otherwise those are dropped (leaked) on a crash.
    void flush_header(bool spill = false);
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
