add_library(
    YdbModes STATIC
    MergingSortedInputStream.cpp
    helpers.cpp
)

add_executable(ut_ydb_modes ut_modes.cpp)
target_link_libraries(ut_ydb_modes YdbModes Common Columns DataStreams
  Arrow::arrow_static
  ch_contrib::cityhash
  ch_contrib::gtest
)
add_test(NAME ydb_modes COMMAND ut_ydb_modes)
