add_library(
    YdbModes STATIC
    ExpressionBlockInputStream.cpp
    CheckSortedBlockInputStream.cpp
    MergingSortedInputStream.cpp
    SortingBlockInputStream.cpp
    SsaProgram.cpp
    helpers.cpp
)

add_executable(ut_ydb_modes tests/ut_modes.cpp)
target_link_libraries(ut_ydb_modes YdbModes Columns DataStreams Common
  Arrow::arrow_static
  ch_contrib::cityhash
  ch_contrib::gtest
)
add_test(NAME ydb_modes COMMAND ut_ydb_modes)

add_executable(ut_ssa_step tests/ut_ssa_step.cpp)
target_link_libraries(ut_ssa_step YdbModes Agg Columns DataStreams Common
  Arrow::arrow_static
  ch_contrib::cityhash
  ch_contrib::gtest
)
add_test(NAME ssa_step COMMAND ut_ssa_step)
