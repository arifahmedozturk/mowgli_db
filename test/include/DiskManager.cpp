#include <gtest/gtest.h>
#include "DiskManager/DiskWriter.h"
#include "DiskManager/DiskReader.h"
#include "Config/Config.h"

#include <fstream>
#include <string>

using namespace std;

TEST(DiskManagerTest, writer_pads_text_correctly) {
  DiskWriter writer;

  string long_string(5000, 'Y');
  EXPECT_EQ(long_string, writer.with_padding(long_string));

  string short_string(10, 'Y');
  string short_string_expected_padding(Config::BLOCK_SIZE - 10, 'X');
  string expected_string = short_string + short_string_expected_padding;
  EXPECT_EQ(expected_string, writer.with_padding(short_string));
}

TEST(DiskManagerTest, writes_to_data_block_correctly) {
  DiskWriter writer;
  DiskReader reader;

  writer.write_string_to_id("test_value", 1, "test.txt");
  string s = reader.read_data_block(1, "test.txt");

  string expected_padding(Config::BLOCK_SIZE - 10, 'X');
  EXPECT_EQ(s.length(), 4096);
  EXPECT_EQ(s, "test_value" + expected_padding);
}