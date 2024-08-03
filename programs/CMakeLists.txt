# 'arrowhouse' binary is a multi purpose tool,
# that contain multiple execution modes (client, server, etc.)
# each of them is built and linked as a separate library, defined below.

option (ENABLE_CLICKHOUSE_ALL "Enable all tools" ON)
option (ENABLE_CLICKHOUSE_LOCAL "Enable arrowhouse-local" ${ENABLE_CLICKHOUSE_ALL})
option (ENABLE_CLICKHOUSE_BENCHMARK "Enable arrowhouse-benchmark" OFF)

if(NOT (MAKE_STATIC_LIBRARIES OR SPLIT_SHARED_LIBRARIES))
    set(CLICKHOUSE_ONE_SHARED 1)
endif()

#configure_file (config_tools.h.in ${ConfigIncludePath}/config_tools.h)

macro(arrowhouse_target_link_split_lib target name)
    if(NOT CLICKHOUSE_ONE_SHARED)
        target_link_libraries(${target} PRIVATE arrowhouse-${name}-lib)
    else()
        target_link_libraries(${target} PRIVATE arrowhouse-lib)
    endif()
endmacro()

macro(arrowhouse_program_link_split_binary name)
    arrowhouse_target_link_split_lib(arrowhouse-${name} ${name})
endmacro()

macro(arrowhouse_program_add_library name)
    string(TOUPPER ${name} name_uc)
    string(REPLACE "-" "_" name_uc ${name_uc})

    # Some dark magic
    set(CLICKHOUSE_${name_uc}_SOURCES ${CLICKHOUSE_${name_uc}_SOURCES} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_LINK ${CLICKHOUSE_${name_uc}_LINK} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_INCLUDE ${CLICKHOUSE_${name_uc}_INCLUDE} PARENT_SCOPE)

    if(NOT CLICKHOUSE_ONE_SHARED)
        add_library(arrowhouse-${name}-lib ${CLICKHOUSE_${name_uc}_SOURCES})

        set(_link ${CLICKHOUSE_${name_uc}_LINK}) # can't use ${} in if()
        if(_link)
            target_link_libraries(arrowhouse-${name}-lib ${CLICKHOUSE_${name_uc}_LINK})
        endif()

        set(_include ${CLICKHOUSE_${name_uc}_INCLUDE}) # can't use ${} in if()
        if (_include)
            target_include_directories(arrowhouse-${name}-lib ${CLICKHOUSE_${name_uc}_INCLUDE})
        endif()
    endif()
endmacro()

macro(arrowhouse_program_add_executable name)
    if(CLICKHOUSE_SPLIT_BINARY)
        add_executable(arrowhouse-${name} clickhouse-${name}.cpp)
        arrowhouse_program_link_split_binary(${name})
        install(TARGETS arrowhouse-${name} ${CLICKHOUSE_ALL_TARGETS} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT arrowhouse)
    endif()
endmacro()

macro(arrowhouse_program_add name)
    arrowhouse_program_add_library(${name})
    arrowhouse_program_add_executable(${name})
endmacro()


add_subdirectory(local)

if (ENABLE_CLICKHOUSE_BENCHMARK)
    add_subdirectory(benchmark)
else()
    set(CLICKHOUSE_BENCHMARK_INCLUDE SYSTEM PRIVATE ${PCG_RANDOM_INCLUDE_DIR})
endif()

if (CLICKHOUSE_ONE_SHARED)
    add_library(arrowhouse-lib SHARED ${CLICKHOUSE_LOCAL_SOURCES} ${CLICKHOUSE_BENCHMARK_SOURCES})
    target_link_libraries(arrowhouse-lib ${CLICKHOUSE_LOCAL_LINK} ${CLICKHOUSE_BENCHMARK_LINK})
    target_include_directories(arrowhouse-lib ${CLICKHOUSE_LOCAL_INCLUDE} ${CLICKHOUSE_BENCHMARK_INCLUDE})
    #set_target_properties(arrowhouse-lib PROPERTIES SOVERSION ${VERSION_MAJOR}.${VERSION_MINOR} VERSION ${VERSION_SO} OUTPUT_NAME arrowhouse DEBUG_POSTFIX "")
endif()

# if (CLICKHOUSE_SPLIT_BINARY)
#     set (CLICKHOUSE_ALL_TARGETS arrowhouse-server arrowhouse-client arrowhouse-local arrowhouse-benchmark arrowhouse-performance-test
#             arrowhouse-extract-from-config arrowhouse-compressor arrowhouse-format arrowhouse-obfuscator arrowhouse-copier)

#     if (ENABLE_CLICKHOUSE_ODBC_BRIDGE)
#         list (APPEND CLICKHOUSE_ALL_TARGETS arrowhouse-odbc-bridge)
#     endif ()

#     #set_target_properties(${CLICKHOUSE_ALL_TARGETS} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)

#     add_custom_target (arrowhouse-bundle ALL DEPENDS ${CLICKHOUSE_ALL_TARGETS})
#     add_custom_target (arrowhouse ALL DEPENDS arrowhouse-bundle)
# else ()
#     add_executable (arrowhouse-x main.cpp)
#     target_link_libraries (arrowhouse-x PRIVATE arrowhouse_common_io)
#     target_include_directories (arrowhouse-x PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

#     if (ENABLE_CLICKHOUSE_LOCAL)
#         arrowhouse_target_link_split_lib(arrowhouse-x local)
#     endif ()
#     if (ENABLE_CLICKHOUSE_BENCHMARK)
#         arrowhouse_target_link_split_lib(arrowhouse-x benchmark)
#     endif ()

#     set (CLICKHOUSE_BUNDLE)
#     if (ENABLE_CLICKHOUSE_LOCAL)
#         add_custom_target (arrowhouse-local ALL COMMAND ${CMAKE_COMMAND} -E create_symlink arrowhouse arrowhouse-local DEPENDS arrowhouse)
#         list(APPEND CLICKHOUSE_BUNDLE arrowhouse-local)
#     endif ()
#     if (ENABLE_CLICKHOUSE_BENCHMARK)
#         add_custom_target (arrowhouse-benchmark ALL COMMAND ${CMAKE_COMMAND} -E create_symlink arrowhouse arrowhouse-benchmark DEPENDS arrowhouse)
#         list(APPEND CLICKHOUSE_BUNDLE arrowhouse-benchmark)
#     endif ()

#     add_custom_target (arrowhouse-bundle ALL DEPENDS ${CLICKHOUSE_BUNDLE})

#     if (USE_GDB_ADD_INDEX)
#         add_custom_command(TARGET arrowhouse POST_BUILD COMMAND ${GDB_ADD_INDEX_EXE} arrowhouse COMMENT "Adding .gdb-index to arrowhouse" VERBATIM)
#     endif()
# endif ()
