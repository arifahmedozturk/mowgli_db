#include <gtest/gtest.h>
#include "DiskManager/DiskWriter.h"
#include "DiskManager/DiskReader.h"

#include <fstream>
#include <vector>

using namespace std;

TEST(DiskWriterTest, gets_values_properly) {
  vector<char> values = {'a', 'b', 'c', 'd'};
  
  DiskWriter w;
  w.write_values_to_file(values, "disk_writer_example.txt");

  DiskReader r;
  EXPECT_EQ(r.get_values_from_file("disk_writer_example.txt"), values);
}