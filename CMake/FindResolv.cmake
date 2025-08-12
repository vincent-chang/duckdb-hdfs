# - Try to find the GNU library (resolv)
#
# Once done this will define
#
#  RESOLV_FOUND - System has gnutls
#  RESOLV_INCLUDE_DIR - The gnutls include directory
#  RESOLV_LIBRARIES - The libraries needed to use gnutls
#  RESOLV_DEFINITIONS - Compiler switches required for using gnutls


FIND_PATH(RESOLV_INCLUDE_DIR resolv.h
	HINTS
		/usr/local
		/usr
		/opt
)
SET(RESOLV_LIBRARY_DIRS "${RESOLV_INCLUDE_DIR}/../lib")
FIND_LIBRARY(RESOLV_LIBRARIES NAMES resolv PATHS "${RESOLV_LIBRARY_DIRS}")

INCLUDE(FindPackageHandleStandardArgs)

# handle the QUIETLY and REQUIRED arguments and set RESOLV_FOUND to TRUE if 
# all listed variables are TRUE
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Resolv DEFAULT_MSG RESOLV_LIBRARIES RESOLV_INCLUDE_DIR)

MARK_AS_ADVANCED(RESOLV_INCLUDE_DIR RESOLV_LIBRARIES)