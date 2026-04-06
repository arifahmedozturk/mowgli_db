cmake_minimum_required(VERSION 3.20)
project(heavy_trie CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# All includes are relative to the project root.
include_directories(${CMAKE_SOURCE_DIR})

find_package(Threads REQUIRED)

add_library(heavy_trie_lib
    index/chain.cpp
    index/disk_trie.cpp
    storage/buffer_pool.cpp
    storage/disk_manager.cpp
    storage/heap.cpp
    catalog/table.cpp
    mql/lexer.cpp
    mql/engine.cpp
)

target_link_libraries(heavy_trie_lib PUBLIC Threads::Threads)

add_executable(mql main.cpp)
target_link_libraries(mql PRIVATE heavy_trie_lib readline)

add_executable(bench bench/bench.cpp)
target_link_libraries(bench PRIVATE heavy_trie_lib)

add_executable(ycsb bench/ycsb.cpp)
target_link_libraries(ycsb PRIVATE heavy_trie_lib)

find_library(LIBPQ_LIB pq)
find_path(LIBPQ_INC libpq-fe.h PATH_SUFFIXES postgresql)
if(LIBPQ_LIB AND LIBPQ_INC)
    add_executable(ycsb_pg bench/ycsb_pg.cpp)
    target_include_directories(ycsb_pg PRIVATE ${LIBPQ_INC})
    target_link_libraries(ycsb_pg PRIVATE ${LIBPQ_LIB})
    message(STATUS "ycsb_pg enabled (libpq found)")
else()
    message(STATUS "ycsb_pg disabled (libpq not found — install libpq-dev)")
endif()

add_executable(server server/server.cpp)
target_link_libraries(server PRIVATE heavy_trie_lib)

add_executable(client server/client.cpp)
target_link_libraries(client PRIVATE readline)

enable_testing()

# ---- tests ----
function(add_test_exe name src)
    add_executable(${name} ${src})
    target_link_libraries(${name} PRIVATE heavy_trie_lib)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

add_test_exe(test_chain_block       tests/test_chain_block.cpp)
add_test_exe(test_chain_match       tests/test_chain_match.cpp)
add_test_exe(test_disk_manager      tests/test_disk_manager.cpp)
add_test_exe(test_disk_trie_lookup  tests/test_disk_trie_lookup.cpp)
add_test_exe(test_disk_trie_insert  tests/test_disk_trie_insert.cpp)
add_test_exe(test_disk_trie_remove  tests/test_disk_trie_remove.cpp)
add_test_exe(test_heap              tests/test_heap.cpp)
add_test_exe(test_table             tests/test_table.cpp)
add_test_exe(test_engine            tests/test_engine.cpp)
