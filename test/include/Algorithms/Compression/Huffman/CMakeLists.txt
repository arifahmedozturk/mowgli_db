cmake_minimum_required(VERSION 3.0)

add_executable(HuffmanNode HuffmanNode.cpp)
target_link_libraries(HuffmanNode PUBLIC
    gtest_main
)
add_test(NAME HuffmanNode COMMAND HuffmanNode)

add_executable(HuffmanEncoder HuffmanEncoder.cpp)
target_link_libraries(HuffmanEncoder PUBLIC
    gtest_main
)
add_test(NAME HuffmanEncoder COMMAND HuffmanEncoder)
