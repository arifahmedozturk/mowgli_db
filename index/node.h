#pragma once
#include <cstdint>

struct Node {
    Node*    children[2] = {};
    uint64_t subtree_size = 0;
    bool     is_terminal = false;

    ~Node() {
        delete children[0];
        delete children[1];
    }
};
