# - Try to find the LibHDFS3 library
#
# Once done this will define
#
#  LibHDFS3_FOUND - System has libhdfs3
#  LIBHDFS3_INCLUDE_DIR - The libhdfs3 include directory
#  LIBHDFS3_LIBRARIES - The libraries needed to use libhdfs3
#  LIBHDFS3_DEFINITIONS - Compiler switches required for using libhdfs3


FIND_PATH(LIBHDFS3_INCLUDE_DIR hdfs/hdfs.h
	HINTS
		/usr/local
		/usr
		/opt
)
SET(LIBHDFS3_LIBRARY_DIRS "${LIBHDFS3_INCLUDE_DIR}/../lib")
FIND_LIBRARY(LIBHDFS3_LIBRARIES NAMES hdfs3 PATHS "${LIBHDFS3_LIBRARY_DIRS}")

INCLUDE(FindPackageHandleStandardArgs)

# handle the QUIETLY and REQUIRED arguments and set LIBHDFS3_FOUND to TRUE if 
# all listed variables are TRUE
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LibHDFS3 DEFAULT_MSG LIBHDFS3_LIBRARIES LIBHDFS3_INCLUDE_DIR)

MARK_AS_ADVANCED(LIBHDFS3_INCLUDE_DIR LIBHDFS3_LIBRARIES)