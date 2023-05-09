#include <gtest/gtest.h>
#include "HeavyTrie/Chain.h"

#include <vector>

using namespace std;

TEST(ChainTest, creates_chain_correctly) {
    vector<char> values = {'a', 'b', 'c'};
    Chain c(values);

    EXPECT_EQ(c.get_values(), values);
}

