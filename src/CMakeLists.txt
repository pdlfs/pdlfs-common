#
# CMakeLists.txt  cmake file for src directory
# 26-May-2016  chuck@ece.cmu.edu
#

#
# this file is either included from ../CMakeLists.txt or some other
# file if we are being embedded within another project.
#

# XXXCDC: config variables

#
# library version set here (e.g. for generating shared libs if we want
#
set (PDLFS_COMMON_VERSION_MAJOR 1)
set (PDLFS_COMMON_VERSION_MINOR 0)
set (PDLFS_COMMON_VERSION_PATCH 0)

#
# provide reasonable defaults for platform/os
#
if (NOT PDLFS_PLATFORM)
  set (PDLFS_PLATFORM POSIX)
endif ()
if (NOT PDLFS_OS)
  set (PDLFS_OS "${CMAKE_SYSTEM_NAME}")
endif ()

string (TOUPPER ${PDLFS_OS} PDLFS_OS_EDITED)
if (${PDLFS_OS_EDITED} STREQUAL "DARWIN")
  # XXX should switch MACOSX to DARWIN
  set (PDLFS_OS_EDITED "MACOSX")
elseif (${PDLFS_OS_EDITED} STREQUAL "HP-UX")
  set (PDLFS_OS_EDITED "HPUX")
elseif (${PDLFS_OS_EDITED} MATCHES "^CYGWIN_")
  set (PDLFS_OS_EDITED "CYGWIN")
endif ()

set (PDLFS_PLATFORM_${PDLFS_PLATFORM} 1)
set (PDLFS_OS_${PDLFS_OS_EDITED} 1)

configure_file ("../include/pdlfs-common/pdlfs_config_expand.h.in"
                "../include/pdlfs-common/pdlfs_config_expand.h" @ONLY)
include_directories ("${CMAKE_CURRENT_BINARY_DIR}/../include")

set (pdlfs-common-srcs arena.cc cache.cc coding.cc crc32c.cc dbfiles.cc
     dbfiles.cc env.cc hash.cc histogram.cc log_reader.cc log_writer.cc
     osd.cc osd_internal.cc port_posix.cc posix_env.cc slice.cc status.cc
     strutil.cc testharness.cc testutil.cc)

set (pdlfs-leveldb-srcs block.cc block_builder.cc bloom.cc comparator.cc
     db/builder.cc db/db.cc db/db_impl.cc db/db_iter.cc db/dbformat.cc
     db/memtable.cc db/options.cc db/repair.cc db/table_cache.cc
     db/version_edit.cc db/version_set.cc db/write_batch.cc filter_block.cc
     filter_policy.cc format.cc iterator.cc merger.cc table.cc
     table_builder.cc table_stats.cc two_level_iterator.cc )
string(REGEX REPLACE "([^;]+)" "leveldb/\\1" pdlfs-leveldb-srcs 
                                          "${pdlfs-leveldb-srcs}")

set (pdlfs-all-srcs ${pdlfs-common-srcs} ${pdlfs-leveldb-srcs})

add_library (pdlfs-common STATIC ${pdlfs-all-srcs})
target_include_directories (pdlfs-common PUBLIC "../include")

set(CMAKE_MACOSX_RPATH 1)
add_library (pdlfs-common-shared SHARED ${pdlfs-all-srcs})
target_include_directories (pdlfs-common-shared PUBLIC "../include")
set (pdlfs-common-shared-vers 
    "${PDLFS_COMMON_VERSION_MAJOR}.${PDLFS_COMMON_VERSION_MINOR}")
set (pdlfs-common-shared-vers 
    "${pdlfs-common-shared-vers}.${PDLFS_COMMON_VERSION_PATCH}")
set_target_properties(pdlfs-common-shared 
                      PROPERTIES VERSION ${pdlfs-common-shared-vers}
                      SOVERSION ${PDLFS_COMMON_VERSION_MAJOR})

install (TARGETS pdlfs-common pdlfs-common-shared
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (DIRECTORY ../include/pdlfs-common
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")
install (FILES 
  "${CMAKE_CURRENT_BINARY_DIR}/../include/pdlfs-common/pdlfs_config_expand.h"
         DESTINATION include/pdlfs-common)
