#include <gtest/gtest.h>
#include "HeavyTrie/HeavyTrie.h"

#include "DiskManager/FileHelper.h"
#include <string>
#include <vector>

using namespace std;

TEST(HeavyTrieTest, creates_root_chains_correctly) {
    FileHelper file_helper;

    HeavyTrie t;
    t.insert("ant");
    t.insert("bag");
    t.insert("cap");

    EXPECT_TRUE(file_helper.file_exists("data/indexes/root/a.txt"));
    EXPECT_TRUE(file_helper.file_exists("data/indexes/root/b.txt"));
    EXPECT_TRUE(file_helper.file_exists("data/indexes/root/c.txt"));
}

