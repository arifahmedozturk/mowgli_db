#pragma once
// A packed block's slot directory (#98): after the block's magic and slot
// count, one 4-byte entry per slot, (offset, length) little-endian. A slot is
// live when both are non-zero; a freed slot keeps its offset with length 0.
// A slot whose bytes start with FORWARD_MAGIC is a forwarding stub: the chain
// moved, and the 8 bytes after the tag are its current address.
#include "index/chain.h"
#include <cstdint>
#include <cstring>

struct SlotEntry {
    uint16_t off = 0;
    uint16_t len = 0;   // the slot's capacity (its size class), 0 = dead
    bool live() const { return off != 0 && len != 0; }
};

// slot is 1-based, as in a chain address.
inline SlotEntry read_slot_entry(const uint8_t* frame, uint8_t slot) {
    SlotEntry e;
    memcpy(&e.off, frame + PACK_DIR_OFFSET + (slot - 1) * 4,     2);
    memcpy(&e.len, frame + PACK_DIR_OFFSET + (slot - 1) * 4 + 2, 2);
    return e;
}

inline void write_slot_entry(uint8_t* frame, uint8_t slot, SlotEntry e) {
    memcpy(frame + PACK_DIR_OFFSET + (slot - 1) * 4,     &e.off, 2);
    memcpy(frame + PACK_DIR_OFFSET + (slot - 1) * 4 + 2, &e.len, 2);
}

// The address a forwarding stub points to, or NULL_BLOCK if the slot holds
// data (or nothing).
inline uint64_t stub_target(const uint8_t* frame, SlotEntry e) {
    if (e.off == 0 || e.len < 10) return NULL_BLOCK;
    uint16_t tag;
    memcpy(&tag, frame + e.off, 2);
    if (tag != FORWARD_MAGIC) return NULL_BLOCK;
    uint64_t fwd;
    memcpy(&fwd, frame + e.off + 2, 8);
    return fwd;
}

// Turn the slot at e into a stub pointing at target (its length becomes 10).
inline void write_stub(uint8_t* frame, uint8_t slot, SlotEntry e, uint64_t target) {
    constexpr uint16_t STUB_LEN = 10;   // 2 (tag) + 8 (addr)
    uint16_t tag = FORWARD_MAGIC;
    write_slot_entry(frame, slot, {e.off, STUB_LEN});
    memcpy(frame + e.off,     &tag,    2);
    memcpy(frame + e.off + 2, &target, 8);
}
