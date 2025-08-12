# - Try to find the GNU library (keyutils)
#
# Once done this will define
#
#  KEYUTILS_FOUND - System has gnutls
#  KEYUTILS_INCLUDE_DIR - The gnutls include directory
#  KEYUTILS_LIBRARIES - The libraries needed to use gnutls
#  KEYUTILS_DEFINITIONS - Compiler switches required for using gnutls


FIND_PATH(KEYUTILS_INCLUDE_DIR keyutils.h
	HINTS
		/usr/local
		/usr
		/opt
)
SET(KEYUTILS_LIBRARY_DIRS "${KEYUTILS_INCLUDE_DIR}/../lib")
FIND_LIBRARY(KEYUTILS_LIBRARIES NAMES keyutils PATHS "${KEYUTILS_LIBRARY_DIRS}")

INCLUDE(FindPackageHandleStandardArgs)

# handle the QUIETLY and REQUIRED arguments and set KEYUTILS_FOUND to TRUE if 
# all listed variables are TRUE
FIND_PACKAGE_HANDLE_STANDARD_ARGS(KeyUtils DEFAULT_MSG KEYUTILS_LIBRARIES KEYUTILS_INCLUDE_DIR)

MARK_AS_ADVANCED(KEYUTILS_INCLUDE_DIR KEYUTILS_LIBRARIES)