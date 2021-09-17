cmake_minimum_required(VERSION 2.8)

project(db)

if (NOT CMAKE_BUILD_TYPE)
    message(STATUS "No build type selected, default to Release")
    set(CMAKE_BUILD_TYPE Release)
else ()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Wall -fgnu-tm")


option(WITH_DCPMM "build with " ON)

# if(WITH_DCPMM) 
# 	message("WITH_DCPMM on, plz ensure you have installed libpmem and libpmemobj on your machine")
# 	add_definitions(-DON_DCPMM)
# 	list(APPEND THIRDPARTY_LIBS -lpmem)
# 	list(APPEND THIRDPARTY_LIBS -lpmemobj)
# endif()

add_library( pmem_rocksdb_db.cc )

include_directories(../lib/pmem-rocksdb/include
	../
)
