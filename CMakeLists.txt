cmake_minimum_required(VERSION 3.10)

project(LSMKV)

set(CMAKE_CXX_STANDARD 17)

add_library(lsmkv INTERFACE)
target_include_directories(lsmkv INTERFACE include)
target_link_libraries(lsmkv INTERFACE stdc++fs)

add_executable(lsmkv_test test/main.cpp)
target_link_libraries(lsmkv_test PRIVATE lsmkv)

add_executable(lsmkv_correctness_test test/correctness.cpp)
target_link_libraries(lsmkv_correctness_test PRIVATE lsmkv)
