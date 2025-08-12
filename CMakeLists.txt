cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME hadoopfs)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(HadoopfsExtension)

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

SET(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})

find_package(Git)
if(Git_FOUND)
  if (NOT DEFINED HADOOPFS_GIT_COMMIT_HASH)
    execute_process(
            COMMAND ${GIT_EXECUTABLE} log -1 --format=%h
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            RESULT_VARIABLE HADOOPFS_GIT_RESULT
            OUTPUT_VARIABLE HADOOPFS_GIT_COMMIT_HASH
            OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()
  execute_process(
          COMMAND ${GIT_EXECUTABLE} describe --tags --abbrev=0
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
          OUTPUT_VARIABLE HADOOPFS_GIT_LAST_TAG
          OUTPUT_STRIP_TRAILING_WHITESPACE)
  execute_process(
          COMMAND ${GIT_EXECUTABLE} describe --tags --long
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
          OUTPUT_VARIABLE HADOOPFS_GIT_ITERATION
          OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
  message("Git NOT FOUND")
endif()

if(HADOOPFS_GIT_RESULT EQUAL "0")
  if(HADOOPFS_GIT_LAST_TAG)
    string(REGEX REPLACE "v([0-9]+).[0-9]+.[0-9]+" "\\1" HADOOPFS_MAJOR_VERSION "${HADOOPFS_GIT_LAST_TAG}")
    string(REGEX REPLACE "v[0-9]+.([0-9]+).[0-9]+" "\\1" HADOOPFS_MINOR_VERSION "${HADOOPFS_GIT_LAST_TAG}")
    string(REGEX REPLACE "v[0-9]+.[0-9]+.([0-9]+)" "\\1" HADOOPFS_PATCH_VERSION "${HADOOPFS_GIT_LAST_TAG}")
  endif()
  string(REGEX REPLACE ".*-([0-9]+)-.*" "\\1" HADOOPFS_DEV_ITERATION "${HADOOPFS_GIT_ITERATION}")
  if(HADOOPFS_DEV_ITERATION EQUAL 0)
    # on a tag; directly use the version
    set(HADOOPFS_VERSION "${HADOOPFS_GIT_LAST_TAG}[${HADOOPFS_GIT_COMMIT_HASH}]")
  else()
    # not on a tag, increment the patch version by one and add a -devX suffix
    math(EXPR HADOOPFS_PATCH_VERSION "${HADOOPFS_PATCH_VERSION}+1")
    set(HADOOPFS_VERSION "v${HADOOPFS_MAJOR_VERSION}.${HADOOPFS_MINOR_VERSION}.${HADOOPFS_PATCH_VERSION}-dev${HADOOPFS_DEV_ITERATION}[${HADOOPFS_GIT_COMMIT_HASH}]")
  endif()
else()
  # fallback for when building from tarball
  set(HADOOPFS_MAJOR_VERSION 0)
  set(HADOOPFS_MINOR_VERSION 0)
  set(HADOOPFS_PATCH_VERSION 1)
  set(HADOOPFS_DEV_ITERATION 0)
  set(HADOOPFS_VERSION "v${HADOOPFS_MAJOR_VERSION}.${HADOOPFS_MINOR_VERSION}.${HADOOPFS_PATCH_VERSION}-dev${HADOOPFS_DEV_ITERATION}[${HADOOPFS_GIT_COMMIT_HASH}]")
endif()

message(STATUS "HadoopFs extension version: ${HADOOPFS_VERSION}")

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

FIND_PACKAGE(OpenSSL REQUIRED)
FIND_PACKAGE(KERBEROS REQUIRED)
IF(OS_LINUX)
  FIND_PACKAGE(LibUUID REQUIRED)
ENDIF(OS_LINUX)

FIND_PACKAGE(LibHDFS3 REQUIRED)
include_directories(BEFORE SYSTEM ${LIBHDFS3_INCLUDE_DIR})

include_directories(src/include)
set(EXTENSION_SOURCES 
    src/hadoopfs.cpp 
    src/hadoopfs_extension.cpp)

set(PARAMETERS "-warnings")
add_definitions(-DHADOOPFS_GIT_COMMIT_HASH="${HADOOPFS_GIT_COMMIT_HASH}")
add_definitions(-DHADOOPFS_VERSION="${HADOOPFS_VERSION}")

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

IF(OS_LINUX)
  target_link_libraries(${TARGET_NAME}_extension ${LIBUUID_LIBRARIES})
  target_link_libraries(${TARGET_NAME}_loadable_extension ${LIBUUID_LIBRARIES})
ENDIF(OS_LINUX)

target_link_libraries(${EXTENSION_NAME} ${KERBEROS_LIBRARIES})
target_link_libraries(${EXTENSION_NAME} ${OPENSSL_LIBRARIES})
target_link_libraries(${EXTENSION_NAME} ${LIBHDFS3_LIBRARIES})

target_link_libraries(${LOADABLE_EXTENSION_NAME} ${KERBEROS_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${OPENSSL_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${LIBHDFS3_LIBRARIES})

install(TARGETS ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")


