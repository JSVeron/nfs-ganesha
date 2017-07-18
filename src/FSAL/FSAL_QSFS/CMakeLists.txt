add_definitions(
  -D_FILE_OFFSET_BITS=64
)
SET(CMAKE_BUILD_TYPE "Debug")

SET(fsalqsfs_LIB_SRCS
  #up.c
  main.c
  export.c
  handle.c
  internal.c
  internal.h
)

message("============== morven ==================")
message("QSFS_INCLUDE_DIR ${QSFS_INCLUDE_DIR}")
include_directories(${QSFS_INCLUDE_DIR})

add_library(fsalqsfs MODULE ${fsalqsfs_LIB_SRCS})
#add_sanitizers(fsalqsfs)

target_link_libraries(fsalqsfs ${QSFS_LIBRARIES} ${SYSTEM_LIBRARIES})

#set_target_properties(fsalqsfs PROPERTIES VERSION 4.2.0 SOVERSION 4)
install(TARGETS fsalqsfs COMPONENT fsal DESTINATION ${FSAL_DESTINATION} )
