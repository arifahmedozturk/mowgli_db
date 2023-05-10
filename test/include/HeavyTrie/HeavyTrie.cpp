#include <gtest/gtest.h>
#include "HeavyTrie/HeavyTrie.h"

#include "DiskManager/FileHelper.h"
#include <string>
#include <vector>

using byte = std::uint8_t;
using namespace std;

class HeavyTrieTest : public ::testing::Test {
    protected:
        virtual void TearDown() {
            FileHelper f;
            f.delete_folder("data");
        }
};

TEST_F(HeavyTrieTest, creates_chains_correctly) {
    FileHelper file_helper;

    HeavyTrie t;
    t.insert("cap");
    t.insert("can");

    EXPECT_TRUE(file_helper.file_exists("data/indexes/can.txt"));
}

TEST_F(HeavyTrieTest, creates_root_chains_correctly) {
    FileHelper file_helper;

    HeavyTrie t;
    t.insert("ant");
    t.insert("bag");
    t.insert("cap");

    EXPECT_TRUE(file_helper.file_exists("data/indexes/root/a.txt"));
    EXPECT_TRUE(file_helper.file_exists("data/indexes/root/b.txt"));
    EXPECT_TRUE(file_helper.file_exists("data/indexes/root/c.txt"));
}
