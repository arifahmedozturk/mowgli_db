#include <gtest/gtest.h>
#include "DiskManager/DiskReader.h"

#include <fstream>
#include <vector>

using namespace std;

void create_mock_file() {
  std::ofstream f("disk_reader_example.txt");
  f << "4 abcd";
  f.close();
}

TEST(DiskReaderTest, gets_values_properly) {
    create_mock_file();

    DiskReader r;
    vector<char> expected_values = {'a', 'b', 'c', 'd'};
    
    EXPECT_EQ(r.get_values_from_file("disk_reader_example.txt"), expected_values);
}