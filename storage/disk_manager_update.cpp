#include "storage/disk_manager.h"
#include "storage/pack_slot.h"
#include <cstring>
#include <mutex>
#include <stdexcept>

// Reading and rewriting chains where they live: in place, through a
// forwarding stub, or moved to a bigger slot behind a new stub.

ChainData DiskManager::read_chain_at(uint64_t chain_addr) const {
    uint8_t  slot = chain_addr_slot(chain_addr);
    uint64_t phys = chain_addr_phys(chain_addr);

    if (slot == 0) {
        // Dedicated block.
        uint8_t buf[BLOCK_SIZE];
        read_block(phys, buf);

        ChainData out;
        if (!chain_decode(buf, out))
            throw std::runtime_error("read_chain_at: invalid dedicated chain");
        return out;
    }

    // Packed block: read directory entry, then the chain slice.
    const uint8_t* frame = pool_.pin_shared(phys, fd_);
    SlotEntry e = read_slot_entry(frame, slot);
    if (!e.live()) {
        pool_.unpin_shared(phys);
        throw std::runtime_error("read_chain_at: slot is dead");
    }
    uint64_t fwd = stub_target(frame, e);
    if (fwd != NULL_BLOCK) {
        pool_.unpin_shared(phys);
        return read_chain_at(fwd); // one hop: stubs always point straight at the data
    }

    ChainData out;
    bool ok = chain_decode_slice(frame + e.off, e.len, out);
    pool_.unpin_shared(phys);
    if (!ok) throw std::runtime_error("read_chain_at: invalid packed chain");
    return out;
}

uint64_t DiskManager::update_chain_at(uint64_t chain_addr, const ChainData& chain) {
    uint8_t tmp[BLOCK_SIZE];
    size_t enc = chain_encode_slice(chain, tmp, sizeof(tmp));
    if (enc == 0) throw std::runtime_error("update_chain_at: chain too large");

    // Dedicated block: anything that encodes fits.
    if (chain_addr_slot(chain_addr) == 0) {
        write_dedicated(chain_addr_phys(chain_addr), tmp, enc);
        return chain_addr;
    }

    SlotEntry e = packed_entry(chain_addr);
    uint64_t fwd;
    {
        uint64_t phys = chain_addr_phys(chain_addr);
        const uint8_t* frame = pool_.pin_shared(phys, fd_);
        fwd = stub_target(frame, e);
        pool_.unpin_shared(phys);
    }
    if (fwd != NULL_BLOCK) return update_forwarded(chain_addr, e, fwd, tmp, enc);

    if (enc <= e.len) {
        // Fits within the slot's reserved capacity — write in-place.
        // Do NOT change the directory len (it records capacity, not current size).
        write_at(chain_addr, e.off, tmp, enc);
        return chain_addr;
    }
    return move_behind_stub(chain_addr, e, tmp, enc);
}

SlotEntry DiskManager::packed_entry(uint64_t chain_addr) const {
    std::lock_guard<std::mutex> lp(pack_mutex_);
    auto it = packed_blocks_.find(chain_addr_phys(chain_addr));
    if (it == packed_blocks_.end())
        throw std::runtime_error("update_chain_at: unknown packed block");
    uint8_t slot = chain_addr_slot(chain_addr);
    return {it->second.offsets[slot - 1], it->second.lengths[slot - 1]};
}

void DiskManager::write_at(uint64_t chain_addr, uint16_t off, const uint8_t* bytes, size_t enc) {
    uint64_t phys = chain_addr_phys(chain_addr);
    uint8_t* frame = pool_.pin_exclusive(phys, fd_);
    memcpy(frame + off, bytes, enc);
    pool_.unpin_exclusive(phys);
}

uint64_t DiskManager::update_forwarded(uint64_t stub_addr, SlotEntry stub, uint64_t fwd,
                                       const uint8_t* bytes, size_t enc) {
    if (chain_addr_slot(fwd) == 0) {           // dedicated: always fits
        write_dedicated(chain_addr_phys(fwd), bytes, enc);
        return fwd;
    }
    SlotEntry f = packed_entry(fwd);
    if (enc <= f.len) { write_at(fwd, f.off, bytes, enc); return fwd; }

    // Outgrew the forwarded slot too: move again, repoint the stub, free the old copy.
    uint64_t moved = place_bytes(bytes, enc, chain_addr_phys(fwd));
    uint64_t phys  = chain_addr_phys(stub_addr);
    uint8_t* frame = pool_.pin_exclusive(phys, fd_);
    write_stub(frame, chain_addr_slot(stub_addr), stub, moved);
    pool_.unpin_exclusive(phys);
    free_chain_slot(fwd);
    return moved;
}

uint64_t DiskManager::move_behind_stub(uint64_t chain_addr, SlotEntry e,
                                       const uint8_t* bytes, size_t enc) {
    // Move to a larger slot (or a dedicated block) and leave a forwarding
    // stub, so parent pointers to chain_addr stay valid.
    uint8_t  slot  = chain_addr_slot(chain_addr);
    uint64_t phys  = chain_addr_phys(chain_addr);
    uint64_t moved = place_bytes(bytes, enc, phys);
    {
        uint8_t* frame = pool_.pin_exclusive(phys, fd_);
        write_stub(frame, slot, e, moved);
        pool_.unpin_exclusive(phys);
    }
    std::lock_guard<std::mutex> lp(pack_mutex_);
    auto it = packed_blocks_.find(phys);
    if (it != packed_blocks_.end()) it->second.lengths[slot - 1] = 10;
    return moved;
}
