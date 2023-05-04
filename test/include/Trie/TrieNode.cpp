#include <gtest/gtest.h>
#include "Trie/TrieNode.h"

TEST(TrieNodeTest, creates_node_correctly) {
    TrieNode leaf(1);

    EXPECT_EQ(leaf.is_key_end(), true);
    EXPECT_EQ(leaf.get_storage_key(), 1);
    EXPECT_EQ(leaf.get_children().size(), 0);

    vector<bool> children;
    children.resize(256, false);
    children[0] = true;
    children[1] = true;
    children[2] = true;

    TrieNode inner_node(children);

    EXPECT_EQ(inner_node.is_key_end(), false);
    EXPECT_EQ(inner_node.get_storage_key(), -1);
    EXPECT_EQ(inner_node.get_children().size(), 3);
}
