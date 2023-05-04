cmake_minimum_required(VERSION 3.0)

set(This Example)

project(${This} CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT")

enable_testing()

add_subdirectory(googletest)

set(Headers
    Example.hpp)

set(Sources
    Example.cpp)

add_library(${This} ${Sources} ${Headers})
include_directories(include)

add_subdirectory(test)