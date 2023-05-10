#include <gtest/gtest.h>
#include "DiskManager/FileHelper.h"

#include <fstream>

using namespace std;

void create_file(string file_name) {
  ofstream outfile;
  outfile.open(file_name.c_str());
  outfile << "This is an example file."; 
  outfile.close();
}

TEST(FileHelperTest, delete_folder_works_correctly) {
  mkdir("delete_folder_test");
  create_file("delete_folder_test/delete_folder_test_file.txt");

  FileHelper f;
  EXPECT_TRUE(f.delete_folder("delete_folder_test"));
  EXPECT_FALSE(f.file_exists("delete_folder_test/delete_folder_test_file.txt"));
}

TEST(FileHelperTest, file_exists_works_correctly) {
  create_file("file_exists_true_test.txt");

  FileHelper f;

  EXPECT_EQ(f.file_exists("file_exists_true_test.txt"), true);
  EXPECT_EQ(f.file_exists("file_exists_false_test.txt"), false);
}