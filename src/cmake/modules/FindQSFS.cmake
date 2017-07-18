# - Find QSFS
# Find the Linux Trace Toolkit - next generation with associated includes path.
# See http://ceph.org/
#
# This module accepts the following optional variables:
#    QSFS_PREFIX   = A hint on QingStor install path.
#
# This module defines the following variables:
#    QSFS_FOUND       = Was QingStor found or not?
#    QSFS_LIBRARIES   = The list of libraries to link to when using QingStor
#    QSFS_INCLUDE_DIR = The path to QingStor include directory
#
# On can set QSFS_PREFIX before using find_package(QSFS) and the
# module with use the PATH as a hint to find QingStor.
#
# The hint can be given on the command line too:
#   cmake -DQSFS_PREFIX=/DATA/ERIC/QSFS /path/to/source


message("============== huang ====================")
if(QSFS_PREFIX)
  message(STATUS "FindQSFS: using PATH HINT: ${QSFS_PREFIX}")
  # Try to make the prefix override the normal paths
  find_path(QSFS_INCLUDE_DIR
    NAMES include/libqingstor.h
    PATHS ${QSFS_PREFIX}
    NO_DEFAULT_PATH
    DOC "The QingStor include headers")

    message("============== huang 1====================")
    message("QSFS_INCLUDE_DIR = ${QSFS_INCLUDE_DIR}")

  find_path(QSFS_LIBRARY_DIR
    NAMES libqsfs.so
    PATHS ${QSFS_PREFIX}
    PATH_SUFFIXES lib lib64
    NO_DEFAULT_PATH
    DOC "The QingStor libraries")
endif()

if (NOT QSFS_INCLUDE_DIR)
# will find lib and headr file in default path
  find_path(QSFS_INCLUDE_DIR
    NAMES include/libqingstor.h
    PATHS ${QSFS_PREFIX}
    DOC "The QingStor include headers")
    message("============== huang 2====================")
    message("QSFS_INCLUDE_DIR = ${QSFS_INCLUDE_DIR}")
endif (NOT QSFS_INCLUDE_DIR)

if (NOT QSFS_LIBRARY_DIR)
# will find lib and headr file in default path
  find_path(QSFS_LIBRARY_DIR
    NAMES qsfs.so
    PATHS ${QSFS_PREFIX}
    PATH_SUFFIXES lib lib64
    DOC "The QingStor libraries")
endif (NOT QSFS_LIBRARY_DIR)

find_library(QSFS_LIBRARY qsfs.so PATHS ${QSFS_LIBRARY_DIR} NO_DEFAULT_PATH)
check_library_exists(qsfs qingstor_mount ${QSFS_LIBRARY_DIR} QSFSLIB)
if (NOT QSFSLIB)
  unset(QSFS_LIBRARY_DIR CACHE)
  unset(QSFS_INCLUDE_DIR CACHE)
endif (NOT QSFSLIB)

set(QSFS_LIBRARIES ${QSFS_LIBRARY})
message(STATUS "Found qsfs libraries: ${QSFS_LIBRARIES}")

#set(QSFS_FILE_HEADER "${QSFS_INCLUDE_DIR}/include/qingstor.h")

# handle the QUIETLY and REQUIRED arguments and set PRELUDE_FOUND to TRUE if
# all listed variables are TRUE

# include(FindPackageHandleStandardArgs)
# FIND_PACKAGE_HANDLE_STANDARD_ARGS(RGW
#   REQUIRED_VARS RGW_INCLUDE_DIR RGW_LIBRARY_DIR
#  VERSION_VAR RGW_FILE_VERSION
#  )

# VERSION FPHSA options not handled by CMake version < 2.8.2)
#                                  VERSION_VAR)

mark_as_advanced(QSFS_INCLUDE_DIR)
mark_as_advanced(QSFS_LIBRARY_DIR)
