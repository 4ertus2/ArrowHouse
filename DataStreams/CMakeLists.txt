add_library(
    DataStreams STATIC
    AggregatingBlockInputStream.cpp
    IBlockInputStream.cpp
    MergingAggregatedBlockInputStream.cpp
)
