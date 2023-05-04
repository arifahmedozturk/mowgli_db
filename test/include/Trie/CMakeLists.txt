cmake_minimum_required(VERSION 3.0)

add_executable(TrieNode TrieNode.cpp)
target_link_libraries(TrieNode PUBLIC
    gtest_main
)
add_test(NAME TrieNode COMMAND TrieNode)