#include "heavy_trie.h"

void HeavyTrie::insert(const std::string& key) {
    if (!root_) root_ = new Node();

    Node* cur = root_;
    cur->subtree_size++;

    for (size_t i = 0; i < bit_len(key); i++) {
        int b = bit_at(key, i);
        if (!cur->children[b])
            cur->children[b] = new Node();
        cur = cur->children[b];
        cur->subtree_size++;
    }

    cur->is_terminal = true;
}

bool HeavyTrie::query(const std::string& key, size_t* chains_out) const {
    if (!root_) return false;

    size_t light_edges = 0;
    Node* cur = root_;

    for (size_t i = 0; i < bit_len(key); i++) {
        int b = bit_at(key, i);
        Node* next = cur->children[b];
        if (!next) return false;

        Node* other = cur->children[1 - b];
        if (other && other->subtree_size >= next->subtree_size)
            light_edges++;

        cur = next;
    }

    if (!cur->is_terminal) return false;
    if (chains_out) *chains_out = light_edges + 1;
    return true;
}
