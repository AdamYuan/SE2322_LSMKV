cmake_minimum_required(VERSION 3.10)

project(LSMKV)

set(CMAKE_CXX_STANDARD 17)

add_library(lsmkv STATIC src/lsmkv.cpp)
target_include_directories(lsmkv PUBLIC include)

add_executable(lsmkv_test test/main.cpp)
target_link_libraries(lsmkv_test PRIVATE lsmkv)
