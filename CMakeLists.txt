cmake_minimum_required(VERSION 3.20)
project(ArrowHouse)
enable_testing()

set(ArrowHouse_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

if("${CMAKE_BUILD_TYPE}" STREQUAL "")
  set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

set(CMAKE_CXX_STANDARD 23)

# gcc -Wno-error=stringop-overflow
set(PEDANTIC "-Werror -Wall -Wextra -Wunreachable-code -Wpointer-arith")
set(IGNORES "-Wno-unused-parameter")
set(MUTED_ERRORS "-Wno-error=redundant-move -Wno-error=deprecated-declarations -Wno-error=array-bounds \
  -Wno-error=missing-field-initializers -Wno-error=ignored-attributes")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pipe -fno-omit-frame-pointer ${PEDANTIC} ${IGNORES}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pipe -fno-omit-frame-pointer ${PEDANTIC} ${MUTED_ERRORS} ${IGNORES}")

find_package(Arrow REQUIRED PATHS ${ArrowHouse_DEPS_PATH})
find_package(GTest)

if (NOT GTest_FOUND)
  set(GTEST_INCLUDE_DIRS ${ArrowHouse_SOURCE_DIR}/contrib/googletest/googletest/include)
endif()

include_directories(
  ${ARROW_INCLUDE_DIR}
  ${GTEST_INCLUDE_DIRS}
  ${ArrowHouse_SOURCE_DIR}
  ${ArrowHouse_SOURCE_DIR}/base
  ${ArrowHouse_SOURCE_DIR}/contrib/cityhash102/include
)

add_subdirectory(contrib)
add_subdirectory(Common)
add_subdirectory(Columns)
add_subdirectory(DataStreams)
add_subdirectory(Functions)
add_subdirectory(YdbModes)

add_library(
  Agg STATIC
  AggregateFunctions/IAggregateFunction.cpp
  Aggregator.cpp

  # used in Common/Allocator
  base/common/mremap.cpp
)

add_library(arrowhouse INTERFACE)
target_link_libraries(arrowhouse INTERFACE
  Agg YdbModes Columns DataStreams Common
  ch_contrib::cityhash
)
target_include_directories(arrowhouse SYSTEM INTERFACE
  ${ArrowHouse_SOURCE_DIR}
  ${ArrowHouse_SOURCE_DIR}/base
)

add_executable(ut_aggregator tests/ut_aggregator.cpp)
target_link_libraries(ut_aggregator arrowhouse Arrow::arrow_static ch_contrib::gtest)
add_test(NAME aggregator COMMAND ut_aggregator)
