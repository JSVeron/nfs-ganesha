project(libqsfs)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
SET(CMAKE_BUILD_TYPE "Debug")


SET(libqsfs_LIB_SRCS
	qs_file.cpp
	qs_file.h
	libqingstor.cpp
	libqingstor.h
)

include_directories(${CUKE_INCLUDE_DIRS} ./include)

add_library(libqsfs ${libqsfs_LIB_SRCS})

target_link_libraries(libqsfs  /root/Github/ganesha4q/src/libqingstor.so /root/Github/ganesha4q/lib/libjsoncpp.so)

#if(GMOCK_FOUND)
    #add_executable(GTestBucketSteps src/GTestBucketSteps.cpp)
    
#endif()

