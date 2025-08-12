# - Try to find the GNU sasl library (gsasl)
#
# Once done this will define
#
#  GSASL_FOUND - System has gnutls
#  GSASL_INCLUDE_DIR - The gnutls include directory
#  GSASL_LIBRARIES - The libraries needed to use gnutls
#  GSASL_DEFINITIONS - Compiler switches required for using gnutls


FIND_PATH(GSASL_INCLUDE_DIR gsasl.h
	HINTS
		/usr/local
		/usr
		/opt
)
SET(GSASL_LIBRARY_DIRS "${GSASL_INCLUDE_DIR}/../lib")
FIND_LIBRARY(GSASL_LIBRARIES NAMES gsasl PATHS "${GSASL_LIBRARY_DIRS}")

INCLUDE(FindPackageHandleStandardArgs)

# handle the QUIETLY and REQUIRED arguments and set GSASL_FOUND to TRUE if 
# all listed variables are TRUE
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GSasl DEFAULT_MSG GSASL_LIBRARIES GSASL_INCLUDE_DIR)

MARK_AS_ADVANCED(GSASL_INCLUDE_DIR GSASL_LIBRARIES)