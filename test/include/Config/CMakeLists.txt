cmake_minimum_required(VERSION 3.0)

add_executable(InfoLoader InfoLoader.cpp)
target_link_libraries(InfoLoader PUBLIC
    gtest_main
)
add_test(NAME InfoLoader COMMAND InfoLoader)