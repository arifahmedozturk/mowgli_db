#include <gtest/gtest.h>
#include "Algorithms/Compression/LZW/LZWDictionary.h"

TEST(LZWDictionaryTest, get) {
    LZWDictionary d(1);

    d.add(1, 2);

    EXPECT_EQ(d.get(1, 2), 0);
    EXPECT_EQ(d.get(2, 3), -1);
}

TEST(LZWDictionaryTest, exists) {
    LZWDictionary d(1);

    d.add(1, 2);

    EXPECT_EQ(d.exists(1, 2), true);
    EXPECT_EQ(d.exists(2, 3), false);
}

TEST(LZWDictionaryTest, resets) {
    LZWDictionary d(1);

    d.add(1, 2);
    d.reset();

    EXPECT_EQ(d.exists(1, 2), false);
}

TEST(LZWDictionaryTest, resets_when_exceeding_size) {
    LZWDictionary d(1);

    d.add(1, 2);
    d.add(2, 3);

    EXPECT_EQ(d.get(1, 2), -1);
    EXPECT_EQ(d.get(2, 3), 0);
}