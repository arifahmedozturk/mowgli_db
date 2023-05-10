cmake_minimum_required(VERSION 3.0)

add_executable(DiskReader DiskReader.cpp)
target_link_libraries(DiskReader PUBLIC
    gtest_main
)
add_test(NAME DiskReader COMMAND DiskReader)

add_executable(DiskWriter DiskWriter.cpp)
target_link_libraries(DiskWriter PUBLIC
    gtest_main
)
add_test(NAME DiskWriter COMMAND DiskWriter)
