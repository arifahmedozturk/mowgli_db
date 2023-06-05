cmake_minimum_required(VERSION 3.0)

add_executable(LZWDictionary LZWDictionary.cpp)
target_link_libraries(LZWDictionary PUBLIC
    gtest_main
)
add_test(NAME LZWDictionary COMMAND LZWDictionary)

add_executable(LZWEncoder LZWEncoder.cpp)
target_link_libraries(LZWEncoder PUBLIC
    gtest_main
)
add_test(NAME LZWEncoder COMMAND LZWEncoder)
