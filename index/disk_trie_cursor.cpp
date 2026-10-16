#include "index/disk_trie.h"
#include "index/disk_trie_internal.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>
#include <shared_mutex>
#include <stdexcept>

// ---- cursor-based in-order traversal ----

// At any record, the accumulated nibbles are byte-aligned (an even count).
static std::string nibbles_to_key(const std::vector<uint8_t>& nibbles) {
    std::string key(nibbles.size() / 2, '\0');
    for (size_t i = 0; i + 1 < nibbles.size(); i += 2)
        key[i / 2] = static_cast<char>(nibbles[i] << 4 | nibbles[i + 1]);
    return key;
}

// The subtrees below a chain node, in key order: its light children plus the
// chain's own continuation (the heavy digit).
struct ChildRef { uint8_t digit; bool heavy; uint64_t block; };

static std::vector<ChildRef> node_children(const ChainData& chain, const ChainNode& node) {
    std::vector<ChildRef> kids;
    kids.reserve(node.lights.size() + 1);
    uint8_t h = chain.path[node.split];
    bool placed = false;
    for (const auto& l : node.lights) {
        if (!placed && h < l.digit) { kids.push_back({h, true, NULL_BLOCK}); placed = true; }
        kids.push_back({l.digit, false, l.block});
    }
    if (!placed) kids.push_back({h, true, NULL_BLOCK});
    return kids;
}

// Push the subtree for kid onto the cursor stack.
static void push_child(TrieCursor& cursor, uint64_t chain_block, const ChainNode& node,
                       const ChildRef& kid, std::vector<uint8_t> prefix) {
    prefix.push_back(kid.digit);
    if (kid.heavy) cursor.stack.push_back({chain_block, static_cast<size_t>(node.split) + 1, std::move(prefix)});
    else           cursor.stack.push_back({kid.block, 0, std::move(prefix)});
}

// advance_leftmost: pop the top frame from cursor.stack and find the leftmost
// (smallest) record in that subtree. Larger sibling subtrees met on the way
// down are pushed so later cursor_next() calls visit them in order.
// Stack is LIFO: stack.back() is always the smallest remaining subtree.
static bool advance_leftmost(const DiskTrie& trie, TrieCursor& cursor,
                              std::string& key_out, RecordPtr& rec_out) {
    while (!cursor.stack.empty()) {
        {
            // A bucket on top is read entry by entry where its frame sits, with
            // the key bytes before it worked out once: no frame pop/push and no
            // per-key prefix, suffix or key allocations (#75).
            auto& top = cursor.stack.back();
            if (!top.cached) top.cached = std::make_shared<const ChainData>(trie.chain_read(top.block_id));
            if (top.cached->bucket) {
                const size_t n = bucket_size(*top.cached);
                if (top.cpos_start >= n) { cursor.stack.pop_back(); continue; }
                if (!top.head_ok) {
                    const size_t whole = top.prefix.size() / 2 * 2;
                    top.head.clear();
                    for (size_t i = 0; i < whole; i += 2)
                        top.head.push_back(static_cast<char>(top.prefix[i] << 4 | top.prefix[i + 1]));
                    top.odd     = top.prefix.size() % 2 ? top.prefix.back() : -1;
                    top.head_ok = true;
                }
                key_out.assign(top.head);
                bucket_entry_append_key(*top.cached, top.cpos_start, top.odd, key_out, rec_out);
                if (++top.cpos_start >= n) cursor.stack.pop_back();
                return true;
            }
        }
        auto frame = std::move(cursor.stack.back());
        cursor.stack.pop_back();
        std::shared_ptr<const ChainData> held = frame.cached;
        const ChainData& chain = *held;

        // First node at or after our resume position.
        size_t ni = 0;
        while (ni < chain.nodes.size() && chain.nodes[ni].split < frame.cpos_start) ni++;

        std::vector<uint8_t> prefix = std::move(frame.prefix);
        size_t pos = frame.cpos_start;

        while (true) {
            const size_t next_split = (ni < chain.nodes.size()) ? chain.nodes[ni].split
                                                                : chain.path_len;
            prefix.insert(prefix.end(), chain.path.begin() + static_cast<ptrdiff_t>(pos),
                          chain.path.begin() + static_cast<ptrdiff_t>(next_split));
            pos = next_split;

            if (ni == chain.nodes.size()) {
                if (chain.tail_record.valid()) {
                    key_out = nibbles_to_key(prefix);
                    rec_out = chain.tail_record;
                    return true;
                }
                break; // empty tail — continue with next stack frame
            }

            const ChainNode& node = chain.nodes[ni];

            if (node.split == chain.path_len) {
                // End node: the tail key (if any) first, then every child in digit order.
                for (size_t k = node.lights.size(); k-- > 0;)
                    push_child(cursor, frame.block_id, node,
                               {node.lights[k].digit, false, node.lights[k].block}, prefix);
                if (chain.tail_record.valid()) {
                    key_out = nibbles_to_key(prefix);
                    rec_out = chain.tail_record;
                    return true;
                }
                break;
            }

            std::vector<ChildRef> kids = node_children(chain, node);

            if (node.record.valid()) {
                // The key ending here is smaller than every child subtree.
                for (size_t k = kids.size(); k-- > 0;)
                    push_child(cursor, frame.block_id, node, kids[k], prefix);
                key_out = nibbles_to_key(prefix);
                rec_out = node.record;
                return true;
            }

            // Visit the smallest child next; push the rest for later.
            for (size_t k = kids.size(); k-- > 1;)
                push_child(cursor, frame.block_id, node, kids[k], prefix);
            if (kids[0].heavy) {
                prefix.push_back(kids[0].digit);
                pos = static_cast<size_t>(node.split) + 1;
                ni++;
            } else {
                push_child(cursor, frame.block_id, node, kids[0], std::move(prefix));
                break; // outer loop pops the light frame just pushed
            }
        }
    }
    return false;
}

// lower_bound_chain: walk chains matching lo nibble by nibble, pushing every
// subtree greater than lo met on the way; then emit the first key >= lo.
static bool lower_bound_chain(const DiskTrie& trie, uint64_t block_id,
                               std::vector<uint8_t> prefix, const std::string& lo,
                               TrieCursor& cursor,
                               std::string& key_out, RecordPtr& rec_out) {
    const size_t lo_n = key_nibbles(lo);

    while (true) { // re-entered when we jump to a light child
        // lo already exhausted: the whole subtree is >= lo.
        if (prefix.size() >= lo_n) {
            cursor.stack.push_back({block_id, 0, std::move(prefix)});
            return advance_leftmost(trie, cursor, key_out, rec_out);
        }

        auto held = std::make_shared<const ChainData>(trie.chain_read(block_id));
        const ChainData& chain = *held;

        if (chain.bucket) {
            // Binary-search the first entry >= the rest of lo, then scan from there.
            std::vector<uint8_t> rest;
            for (size_t k = prefix.size(); k < lo_n; k++) rest.push_back(key_nibble(lo, k));
            size_t idx = bucket_lower_bound(chain, rest);
            cursor.stack.push_back({block_id, idx, std::move(prefix), held});
            return advance_leftmost(trie, cursor, key_out, rec_out);
        }

        size_t ni     = 0;
        bool   jumped = false;

        for (size_t i = 0; i < chain.path_len; i++) {
            const bool   has_node = ni < chain.nodes.size() && chain.nodes[ni].split == i;
            const size_t key_pos  = prefix.size();

            // lo is exhausted here — everything from this position on is >= lo.
            if (key_pos == lo_n) {
                cursor.stack.push_back({block_id, i, std::move(prefix), held});
                return advance_leftmost(trie, cursor, key_out, rec_out);
            }

            const uint8_t lo_d = key_nibble(lo, key_pos);
            const uint8_t cd   = chain.path[i];

            if (has_node) {
                // A record here is a key shorter than lo, so smaller: skip it.
                const ChainNode& node = chain.nodes[ni];
                std::vector<ChildRef> kids = node_children(chain, node);
                for (size_t k = kids.size(); k-- > 0;)
                    if (kids[k].digit > lo_d) push_child(cursor, block_id, node, kids[k], prefix);

                if (lo_d == cd) { prefix.push_back(cd); ni++; continue; }
                int li = node.light_index(lo_d);
                if (li < 0) return advance_leftmost(trie, cursor, key_out, rec_out);
                prefix.push_back(lo_d);
                block_id = node.lights[li].block;
                jumped = true;
                break;
            }
            if (lo_d < cd) {
                // The chain's digit is above lo's: this and all deeper keys are > lo.
                cursor.stack.push_back({block_id, i, std::move(prefix), held});
                return advance_leftmost(trie, cursor, key_out, rec_out);
            }
            if (lo_d > cd) return advance_leftmost(trie, cursor, key_out, rec_out);
            prefix.push_back(cd);
        }

        if (jumped) continue;

        // Every nibble of the chain matched lo.
        if (prefix.size() >= lo_n) {
            // lo ends here: the tail key and any end-node children are all >= lo.
            cursor.stack.push_back({block_id, chain.path_len, std::move(prefix), held});
            return advance_leftmost(trie, cursor, key_out, rec_out);
        }
        // lo continues past the chain: the tail key (if any) is shorter, so smaller.
        if (chain.has_end_node()) {
            const ChainNode& end  = chain.nodes.back();
            const uint8_t    lo_d = key_nibble(lo, prefix.size());
            for (size_t k = end.lights.size(); k-- > 0;)
                if (end.lights[k].digit > lo_d)
                    push_child(cursor, block_id, end, {end.lights[k].digit, false, end.lights[k].block}, prefix);
            int li = end.light_index(lo_d);
            if (li < 0) return advance_leftmost(trie, cursor, key_out, rec_out);
            prefix.push_back(lo_d);
            block_id = end.lights[li].block;
            continue;
        }
        return advance_leftmost(trie, cursor, key_out, rec_out);
    }
}

void DiskTrie::range_visit(const std::string& lo, const std::string& hi,
                           const std::function<bool(const std::string&, RecordPtr)>& fn) const {
    std::shared_lock<std::shared_mutex> lock(trie_latch_);
    if (dm_.root_block() == NULL_BLOCK) return;

    TrieCursor  cursor;
    std::string key;   // reused for every key
    RecordPtr   rec{NULL_BLOCK, 0};

    if (!lower_bound_chain(*this, dm_.root_block(), {}, lo, cursor, key, rec))
        return;
    do {
        if (key > hi || !fn(key, rec)) return;
    } while (advance_leftmost(*this, cursor, key, rec));
}

void DiskTrie::range_scan(const std::string& lo, const std::string& hi,
                          std::vector<std::pair<std::string, RecordPtr>>& out) const {
    range_visit(lo, hi, [&](const std::string& key, RecordPtr rec) { out.push_back({key, rec}); return true; });
}

bool DiskTrie::lower_bound(const std::string& lo, TrieCursor& cursor) const {
    std::shared_lock<std::shared_mutex> lock(trie_latch_);
    cursor = TrieCursor{};
    if (dm_.root_block() == NULL_BLOCK) return false;
    if (!lower_bound_chain(*this, dm_.root_block(), {}, lo,
                           cursor, cursor.pending_key, cursor.pending_rec))
        return false;
    cursor.has_pending = true;
    return true;
}

bool DiskTrie::cursor_next(TrieCursor& cursor, std::string& key, RecordPtr& rec) const {
    if (cursor.has_pending) {
        key = std::move(cursor.pending_key);
        rec = cursor.pending_rec;
        cursor.has_pending = false;
        return true;
    }
    std::shared_lock<std::shared_mutex> lock(trie_latch_);
    return advance_leftmost(*this, cursor, key, rec);
}

size_t DiskTrie::active_chain_count() const {
    std::shared_lock<std::shared_mutex> lock(trie_latch_);
    if (dm_.root_block() == NULL_BLOCK) return 0;

    size_t count = 0;
    std::vector<uint64_t> stack = {dm_.root_block()};
    while (!stack.empty()) {
        uint64_t block_id = stack.back();
        stack.pop_back();
        count++;
        ChainData chain = chain_read_shared(block_id);
        for (const auto& node : chain.nodes)
            for (const auto& l : node.lights) stack.push_back(l.block);
    }
    return count;
}

