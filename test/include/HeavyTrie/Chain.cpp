#include <gtest/gtest.h>
#include "HeavyTrie/Chain.h"
#include "DiskManager/DiskReader.h"
#include "DiskManager/DiskWriter.h"

#include <vector>

using namespace std;

TEST(ChainTest, creates_chain_correctly_from_file) {
    vector<char> values = {'a', 'b', 'c'};
    DiskWriter w;
    w.write_values_to_file(values, "chain_constructor_example.txt");
    Chain c("chain_constructor_example.txt");

    EXPECT_EQ(c.get_values(), values);
}

TEST(ChainTest, creates_chain_correctly_from_numbers) {
    vector<char> values = {'a', 'b', 'c'};
    Chain c(values);

    EXPECT_EQ(c.get_values(), values);
}

TEST(ChainTest, gets_match_from_chain_correctly) {
    vector<char> values = {'a', 'b', 'a', 'c', 'd', 'a'};
    Chain c(values);

    EXPECT_EQ(c.get_match_from_word("abad"), "aba");
    EXPECT_EQ(c.get_match_from_word("abacdada"), "abacda");
    EXPECT_EQ(c.get_match_from_word("bad"), "");
}

TEST(ChainTest, writes_chain_to_file_correctly) {
    vector<char> values = {'a', 'b', 'c'};
    Chain c(values);
    c.write_chain_to_file("chain_write_example.txt");

    DiskReader r;
    EXPECT_EQ(r.get_values_from_file("chain_write_example.txt"), values);
}

