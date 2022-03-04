cmake_minimum_required(VERSION 3.15)

project(ArangoRocks)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(BuildVelocyPackExamples OFF CACHE BOOL "Flag if velocypack examples are built.")
set(BuildTools OFF CACHE BOOL "Flag if velocypack tools are built.")
set(BuildTests OFF CACHE BOOL "Flag if velocypack tests are built.")
set(BuildAsmTest OFF CACHE BOOL "Flag if velocypack asm tests are built.")

set(WITH_TOOLS ON CACHE BOOL "enable tools")
set(WITH_BENCHMARK OFF CACHE BOOL "disable benchmark tools")
set(WITH_TESTS OFF CACHE BOOL "disable tests")
set(WITH_ALL_TESTS OFF CACHE BOOL "disable all tests")
set(WITH_EXAMPLES OFF CACHE BOOL "disable examples")
set(WITH_SNAPPY ON CACHE BOOL "enable snappy")
set(WITH_LIBURING OFF CACHE BOOL "disable io_uring")
set(WITH_FOLLY_DISTRIBUTED_MUTEX OFF CACHE BOOL "disable folly")
set(uring_LIBRARIES "${CMAKE_SOURCE_DIR}/liburing/src/liburing.a" CACHE STRING "uring lib")
set(uring_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/liburing/src/include" CACHE STRING "uring include")

find_package(Threads REQUIRED)

add_subdirectory(rocksdb)
add_subdirectory(velocypack)
add_subdirectory(docopt.cpp)

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3")

link_directories("${CMAKE_SOURCE_DIR}/liburing/src")

add_executable(arangorocks
    arangorocks.cpp)

target_include_directories(arangorocks
    PUBLIC 
    rocksdb/include
    velocypack/include
    docopt.cpp
    ${CMAKE_SOURCE_DIR}/liburing/src
)

target_link_libraries(arangorocks
    velocypack
    rocksdb
    dl
    docopt
    uring
)

