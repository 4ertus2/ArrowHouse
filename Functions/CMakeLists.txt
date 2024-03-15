list (APPEND PUBLIC_LIBS
    ch_contrib::wyhash
    ch_contrib::cityhash
)

list (APPEND PRIVATE_LIBS
    ch_contrib::xxHash
)

add_executable(ut_hashes_smoke tests/gtest_hashes_smoke.cpp)
target_link_libraries(ut_hashes_smoke
    ${PUBLIC_LIBS} ${PRIVATE_LIBS}
    Arrow::arrow_static
    ch_contrib::gtest
)
add_test(NAME hashes_smoke COMMAND ut_hashes_smoke)
