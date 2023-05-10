#include <gtest/gtest.h>
#include "HeavyTrie/Chain.h"
#include "DiskManager/DiskReader.h"

#include <vector>

using namespace std;

TEST(ChainTest, creates_chain_correctly) {
    vector<char> values = {'a', 'b', 'c'};
    Chain c(values);

    EXPECT_EQ(c.get_values(), values);
}

TEST(ChainTest, writes_chain_to_file_correctly) {
    vector<char> values = {'a', 'b', 'c'};
    Chain c(values);
    c.write_chain_to_file("chain_write_example.txt");

    DiskReader r;
    EXPECT_EQ(r.get_values_from_file("chain_write_example.txt"), values);
}

