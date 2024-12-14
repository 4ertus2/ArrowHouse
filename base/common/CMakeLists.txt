set (SRCS
    demangle.cpp
    errnoToString.cpp
    getPageSize.cpp
    itoa.cpp
    mremap.cpp
    shift10.cpp
)

add_library (arrowhouse_base ${SRCS})

if (WITH_COVERAGE)
    target_compile_definitions(arrowhouse_base PUBLIC WITH_COVERAGE=1)
else ()
    target_compile_definitions(arrowhouse_base PUBLIC WITH_COVERAGE=0)
endif ()

target_include_directories(arrowhouse_base PUBLIC .. "${CMAKE_CURRENT_BINARY_DIR}/..")
