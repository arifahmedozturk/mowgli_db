#include <gtest/gtest.h>
#include <unordered_map>
#include "Algorithms/Compression/Huffman/HuffmanNode.h"

TEST(HuffmanNodeTest, creates_nodes_correctly) {
    HuffmanNode* x = new HuffmanNode(1, 1);
    HuffmanNode* y = new HuffmanNode(2, 1);
    HuffmanNode* z = new HuffmanNode(x, y);

    EXPECT_EQ(x->symbol, 1);
    EXPECT_EQ(y->symbol, 2);
    EXPECT_EQ(z->symbol, -1);
}

TEST(HuffmanNodeTest, creates_translation_dictionary_correctly) {
    HuffmanNode* x = new HuffmanNode(1, 1);
    HuffmanNode* y = new HuffmanNode(2, 1);
    HuffmanNode* z = new HuffmanNode(x, y);

    unordered_map<int, string> translation_dictionary = z->get_translation_dictionary();

    EXPECT_EQ(translation_dictionary.find(x->symbol) == translation_dictionary.end(), false);
    EXPECT_EQ(translation_dictionary.find(y->symbol) == translation_dictionary.end(), false);
    EXPECT_EQ(translation_dictionary.find(z->symbol) == translation_dictionary.end(), true);
    
    EXPECT_EQ(translation_dictionary[x->symbol], "0");
    EXPECT_EQ(translation_dictionary[y->symbol], "1");
}
