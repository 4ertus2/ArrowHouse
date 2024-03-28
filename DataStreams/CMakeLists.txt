add_library(
    DataStreams STATIC
    AggregatingBlockInputStream.cpp
    FilterColumnsBlockInputStream.cpp
    IBlockInputStream.cpp
    MergeSortingBlockInputStream.cpp
    MergingAggregatedBlockInputStream.cpp
    ReverseBlockInputStream.cpp
    ParquetBlockInputStream.cpp
)

add_executable(ut_streams_smoke tests/gtest_streams_smoke.cpp)
target_link_libraries(ut_streams_smoke
    DataStreams YdbModes Common
    Arrow::arrow_static
    ch_contrib::gtest
)
add_test(NAME streams_smoke COMMAND ut_streams_smoke)
