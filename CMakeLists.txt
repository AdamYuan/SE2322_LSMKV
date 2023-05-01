cmake_minimum_required(VERSION 3.10)

project(LSMKV)

set(CMAKE_CXX_STANDARD 17)

option(LSMKV_BUILD_TESTS "Build tests" ON)

add_library(lsmkv INTERFACE)
target_include_directories(lsmkv INTERFACE include)
# target_link_libraries(lsmkv INTERFACE stdc++fs) Not needed for new compilers

if (LSMKV_BUILD_TESTS)
    add_subdirectory(test/lz4)
    add_subdirectory(test/snappy)

    add_executable(lsmkv_correctness test/correctness.cpp)
    target_link_libraries(lsmkv_correctness PRIVATE lsmkv lz4 snappy)

    add_executable(lsmkv_persistence test/persistence.cpp)
    target_link_libraries(lsmkv_persistence PRIVATE lsmkv lz4 snappy)

    find_package(Matplot++)
    if (Matplot++_FOUND)
        add_executable(lsmkv_prof_regular test/prof_regular.cpp)
        target_link_libraries(lsmkv_prof_regular PRIVATE lsmkv Matplot++::matplot)

        add_executable(lsmkv_prof_cache test/prof_cache.cpp)
        target_link_libraries(lsmkv_prof_cache PRIVATE lsmkv Matplot++::matplot)

        add_executable(lsmkv_prof_compaction test/prof_compaction.cpp)
        target_link_libraries(lsmkv_prof_compaction PRIVATE lsmkv Matplot++::matplot)

        add_executable(lsmkv_prof_level test/prof_level.cpp)
        target_link_libraries(lsmkv_prof_level PRIVATE lsmkv Matplot++::matplot)
    endif ()
endif ()