cmake_minimum_required(VERSION 3.0)

add_executable(DiskManager DiskManager.cpp)
target_link_libraries(DiskManager PUBLIC
    gtest_main
)
add_test(NAME DiskManager COMMAND DiskManager)