cmake_minimum_required(VERSION 3.0)

add_executable(Chain Chain.cpp)
target_link_libraries(Chain PUBLIC
    gtest_main
)
add_test(NAME Chain COMMAND Chain)

add_executable(HeavyTrie HeavyTrie.cpp)
target_link_libraries(HeavyTrie PUBLIC
    gtest_main
)
add_test(NAME HeavyTrie COMMAND HeavyTrie)
