cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME hadoopfs)
project(${TARGET_NAME})

set(OS_NAME "unknown")
set(OS_ARCH "amd64")

string(REGEX MATCH "(arm64|aarch64)" IS_ARM "${CMAKE_SYSTEM_PROCESSOR}")
if (IS_ARM)
    set(OS_ARCH "arm64")
elseif (FORCE_32_BIT)
    set(OS_ARCH "i386")
endif ()

if (APPLE)
    set(OS_NAME "osx")
endif ()
if (WIN32)
    set(OS_NAME "windows")
endif ()
if (UNIX AND NOT APPLE)
    set(OS_NAME "linux") # sorry BSD
endif ()

set(LIBHDFS3_HOME "${CMAKE_CURRENT_SOURCE_DIR}/third_party/libhdfs3")
if (NOT EXISTS "${LIBHDFS3_HOME}")
    execute_process(
            COMMAND bash libhdfs3-config.sh
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif ()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_CURRENT_SOURCE_DIR})
find_package(LIBHDFS3 REQUIRED)
include_directories(${LIBHDFS3_INCLUDE_DIRS})
include_directories(src/include)

set(EXTENSION_SOURCES src/hadoopfs.cpp src/hadoopfs_extension.cpp)

# build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(hadoopfs_loadable_extension ${LIBHDFS3_LIBRARIES})

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")


