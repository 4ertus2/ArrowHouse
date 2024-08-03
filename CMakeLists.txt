cmake_minimum_required(VERSION 3.20)
project(ArrowHouse)
enable_testing()

set(ArrowHouse_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

if("${CMAKE_BUILD_TYPE}" STREQUAL "")
  set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

set (CMAKE_CXX_STANDARD 23)
#set (CMAKE_CXX_EXTENSIONS OFF)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

set (CMAKE_C_STANDARD 11)
#set (CMAKE_C_EXTENSIONS ON)
set (CMAKE_C_STANDARD_REQUIRED ON)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -pipe -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -pipe -fno-omit-frame-pointer")

add_subdirectory(contrib EXCLUDE_FROM_ALL)

# gcc -Wno-error=stringop-overflow
set(PEDANTIC "-Werror -Wall -Wextra -Wunreachable-code -Wpointer-arith")
set(IGNORES "-Wno-unused-parameter")
set(MUTED_ERRORS "-Wno-error=redundant-move -Wno-error=deprecated-declarations -Wno-error=array-bounds \
  -Wno-error=missing-field-initializers -Wno-error=ignored-attributes")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${PEDANTIC} ${IGNORES}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PEDANTIC} ${MUTED_ERRORS} ${IGNORES}")

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
  ${ArrowHouse_SOURCE_DIR}/base/pcg-random
  ${ArrowHouse_SOURCE_DIR}/contrib/cityhash102/include
  ${ArrowHouse_SOURCE_DIR}/contrib/fmtlib/include
  ${ArrowHouse_SOURCE_DIR}/contrib/double-conversion
  ${ArrowHouse_SOURCE_DIR}/contrib/fast_float/include
  ${ArrowHouse_SOURCE_DIR}/contrib/libunwind/include
  ${ArrowHouse_SOURCE_DIR}/contrib/dragonbox/include
)

add_subdirectory(base)

add_library(
  arrowhouse_common_io STATIC
  Common/Allocator.cpp
  Common/PODArray.cpp
  Common/Exception.cpp
  Common/UUID.cpp
  # Common/StackTrace and deps
  Common/StackTrace.cpp
  Common/Dwarf.cpp
  Common/Elf.cpp
  Common/SymbolIndex.cpp
  #
  Common/StringUtils.cpp
  Common/projection.cpp
  #
  IO/CompressionMethod.cpp
  IO/DoubleConverter.cpp
  IO/MMappedFileDescriptor.cpp
  IO/MMapReadBufferFromFile.cpp
  IO/MMapReadBufferFromFileDescriptor.cpp
  IO/readFloatText.cpp
  IO/ReadBuffer.cpp
  IO/ReadBufferFromFileBase.cpp
  IO/ReadBufferFromFileDescriptor.cpp
  IO/ReadBufferFromFileDecorator.cpp
  IO/ReadBufferFromMemory.cpp
  IO/ReadHelpers.cpp
  IO/SeekableReadBuffer.cpp
  IO/VarInt.cpp
  IO/WriteBufferFromFileBase.cpp
  IO/WriteBufferFromFileDescriptor.cpp
  IO/WriteHelpers.cpp
  IO/ZlibInflatingReadBuffer.cpp
  IO/ZlibDeflatingWriteBuffer.cpp
  IO/WithFileName.cpp
  IO/WithFileSize.cpp
)
target_link_libraries(arrowhouse_common_io INTERFACE
  arrowhouse_base
  ch_contrib::fmt
  ch_contrib::fast_float
  ch_contrib::double_conversion
  ch_contrib::dragonbox_to_chars
  ch_contrib::cityhash
  pcg_random
  unwind
)

add_subdirectory(Columns)
add_subdirectory(DataStreams)
add_subdirectory(Functions)
add_subdirectory(YdbModes)
add_subdirectory(programs)

add_library(
  arrowhouse_aggregator STATIC
  AggregateFunctions/IAggregateFunction.cpp
  Interpreters/Aggregator.cpp

  # used in Common/Allocator
  base/common/mremap.cpp
)

add_library(arrowhouse INTERFACE)
target_link_libraries(arrowhouse INTERFACE
  arrowhouse_aggregator
  arrowhouse_ydbmodes
  arrowhouse_columns
  arrowhouse_datastreams
  arrowhouse_common_io
)
target_include_directories(arrowhouse SYSTEM INTERFACE
  ${ArrowHouse_SOURCE_DIR}
  ${ArrowHouse_SOURCE_DIR}/base
)

add_executable(ut_aggregator tests/ut_aggregator.cpp)
target_link_libraries(ut_aggregator arrowhouse Arrow::arrow_static ch_contrib::gtest)
add_test(NAME aggregator COMMAND ut_aggregator)
