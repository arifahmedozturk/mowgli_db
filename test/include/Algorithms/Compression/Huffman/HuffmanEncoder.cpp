#include <gtest/gtest.h>
#include <vector>
#include "Algorithms/Compression/Huffman/HuffmanEncoder.h"

using namespace std;

TEST(HuffmanEncoderTest, gets_encoding_string_from_numbers) {
    vector<int> v = { 1, 2, 3, 1};
    HuffmanEncoder h;

    string string_answer = h.get_encoding_string_from_numbers(v);
    
    EXPECT_EQ(string_answer, "01000000010100000010100000011010110");
}