cmake_minimum_required(VERSION 3.0)

add_executable(DiskReader DiskReader.cpp)
target_link_libraries(DiskReader PUBLIC
    gtest_main
)
add_test(NAME DiskReader COMMAND DiskReader)
