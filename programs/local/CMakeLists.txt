set(CLICKHOUSE_LOCAL_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/LocalServer.cpp)
set(CLICKHOUSE_LOCAL_LINK PRIVATE arrowhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
#set(CLICKHOUSE_LOCAL_INCLUDE SYSTEM PRIVATE ...)

arrowhouse_program_add(local)

if(NOT CLICKHOUSE_ONE_SHARED)
    target_link_libraries(clickhouse-local-lib PRIVATE clickhouse-server-lib)
endif()
