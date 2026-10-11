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
    storage/wal.cpp
    catalog/table.cpp
    mql/lexer.cpp
    mql/engine.cpp
)

target_link_libraries(heavy_trie_lib PUBLIC Threads::Threads)

add_executable(mql main.cpp)
target_link_libraries(mql PRIVATE heavy_trie_lib readline)

add_executable(bench bench/bench.cpp)
target_link_libraries(bench PRIVATE heavy_trie_lib)

add_executable(bench_fs bench/bench_fs.cpp)

# B-tree baselines (LMDB / SQLite): built when either dev package is installed.
find_library(LMDB_LIB lmdb)
find_path(LMDB_INC lmdb.h)
find_library(SQLITE_LIB sqlite3)
find_path(SQLITE_INC sqlite3.h)
if((LMDB_LIB AND LMDB_INC) OR (SQLITE_LIB AND SQLITE_INC))
    add_executable(bench_btree bench/bench_btree.cpp)
    if(LMDB_LIB AND LMDB_INC)
        target_include_directories(bench_btree PRIVATE ${LMDB_INC})
        target_link_libraries(bench_btree PRIVATE ${LMDB_LIB})
        target_compile_definitions(bench_btree PRIVATE HAVE_LMDB)
    endif()
    if(SQLITE_LIB AND SQLITE_INC)
        target_include_directories(bench_btree PRIVATE ${SQLITE_INC})
        target_link_libraries(bench_btree PRIVATE ${SQLITE_LIB})
        target_compile_definitions(bench_btree PRIVATE HAVE_SQLITE)
    endif()
    message(STATUS "bench_btree enabled (lmdb: ${LMDB_LIB}, sqlite: ${SQLITE_LIB})")
else()
    message(STATUS "bench_btree disabled (install liblmdb-dev and/or libsqlite3-dev)")
endif()

add_executable(bench_wal bench/bench_wal.cpp)
target_link_libraries(bench_wal PRIVATE heavy_trie_lib)

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

add_executable(server server/server.cpp server/cluster.cpp server/repl.cpp)
target_link_libraries(server PRIVATE heavy_trie_lib)

add_executable(client server/client.cpp)
target_link_libraries(client PRIVATE readline)

enable_testing()

# ---- tests ----
# Tests rely on assert(), so keep it enabled even in Release/RelWithDebInfo builds.
function(add_test_exe name src)
    add_executable(${name} ${src} ${ARGN})
    target_link_libraries(${name} PRIVATE heavy_trie_lib)
    target_compile_options(${name} PRIVATE -UNDEBUG)
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
add_test_exe(test_compact           tests/test_compact.cpp)
add_test_exe(test_wal               tests/test_wal.cpp)

add_test_exe(test_repl              tests/test_repl.cpp server/repl.cpp)
add_test_exe(test_wire              tests/test_wire.cpp)
add_test_exe(test_concurrent_reads  tests/test_concurrent_reads.cpp)
add_test_exe(test_large_file       tests/test_large_file.cpp)
