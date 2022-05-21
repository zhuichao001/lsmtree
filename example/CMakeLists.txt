cmake_minimum_required(VERSION 3.10)

project(default C CXX)

add_definitions(-std=c++14)

set(CXX_FLAGS
 -g
 -DCHECK_PTHREAD_RETURN_VALUE
 -D_FILE_OFFSET_BITS=64
 -Wall
 -Wextra
 -Werror
 -march=native
 -std=c++14
 -rdynamic
 )

include_directories(../../../zhuichao001/naughty/fio)
include_directories(../../../zhuichao001/wal)

include_directories(../src/)
add_subdirectory(../src lsmtree_lib_dir)

add_executable(default ./default.cpp)
target_link_libraries(default lsmtree pthread)
