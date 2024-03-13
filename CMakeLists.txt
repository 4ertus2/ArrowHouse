cmake_minimum_required(VERSION 3.20)
project(arrow_house)
enable_testing()

if("${CMAKE_BUILD_TYPE}" STREQUAL "")
  set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

set(CMAKE_CXX_STANDARD 20)

set(PEDANTIC "-Werror -Wall -Wextra -Wshadow -Wunreachable-code -Wpointer-arith")
set(IGNORES "-Wno-ignored-attributes -Wno-unused-parameter -Wno-missing-field-initializers -Wno-shadow")
set(MUTED_ERRORS "")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pipe ${PEDANTIC} ${MUTED_ERRORS} ${IGNORES}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pipe ${PEDANTIC} ${MUTED_ERRORS} ${IGNORES}")

find_package(Arrow REQUIRED PATHS ${DEPS_PATH})
find_package(GTest)

if (NOT GTest_FOUND)
  set(GTEST_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/contrib/googletest/googletest/include)
endif()

include_directories(
  ${ARROW_INCLUDE_DIR}
  ${GTEST_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/base
  ${CMAKE_SOURCE_DIR}/contrib/cityhash102/include
)

add_subdirectory(contrib)
add_subdirectory(Common)
add_subdirectory(Columns)
add_subdirectory(DataStreams)

add_library(
  arrow_house STATIC
  AggregateFunctions/IAggregateFunction.cpp
  Aggregator.cpp

  # used in Common/Allocator
  base/common/mremap.cpp
)

#contrib/cityhash-1.0.2
#include base

add_executable(ut_aggregator ut_aggregator.cpp)
target_link_libraries(ut_aggregator arrow_house Common Columns DataStreams
  Arrow::arrow_static
  ch_contrib::cityhash
  ch_contrib::gtest
)
add_test(NAME aggregator COMMAND ut_aggregator)
