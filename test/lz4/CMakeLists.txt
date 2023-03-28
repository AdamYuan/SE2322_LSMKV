cmake_minimum_required(VERSION 3.10)

project(LZ4)

add_library(lz4 STATIC lz4.c)
target_include_directories(lz4 INTERFACE .)