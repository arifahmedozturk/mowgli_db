#include <gtest/gtest.h>
#include <string>
#include <vector>
#include "Algorithms/Compression/LZW/LZWEncoder.h"

using namespace std;

TEST(LZWEncoder, get_encoding_string_from_one_number) {
    LZWEncoder e;
    vector<int> v = { 0 };

    string s = e.get_encoding_string_from_numbers(v);

    EXPECT_EQ(s, "000000000");
}

TEST(LZWEncoder, get_encoding_string_test_pattern) {
    LZWEncoder e;
    vector<int> v = { 0, 1, 0, 1 };

    string s = e.get_encoding_string_from_numbers(v);

    EXPECT_EQ(s, "00000000000000000110000000000");
}