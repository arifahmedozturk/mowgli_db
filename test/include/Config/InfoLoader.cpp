#include <gtest/gtest.h>
#include "Config/InfoLoader.h"

#include <fstream>
#include <string>

using namespace std;

TEST(InfoLoaderTest, reads_info_correctly) {
    ofstream test_file("test_file.mwg");
    test_file << 3;
    test_file.close();

    InfoLoader f("test_file.mwg");
    EXPECT_EQ(f.get_free_data_block_id(), 3);
}