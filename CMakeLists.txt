cmake_minimum_required(VERSION 3.20)
project(ycsb)

set(CMAKE_CXX_STANDARD 17)

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
include(FindThreads)
find_package(PkgConfig REQUIRED)
pkg_check_modules(TBB REQUIRED tbb)
pkg_check_modules(LIBPMEM REQUIRED libpmem)
pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)


# only on DB can be set to ON when building ycsb
#as some function name may conflict
option(WITH_PMEM_ROCKSDB "support the pmem rocksdb" OFF)
option(WITH_METAKV "support for MetaKV" ON)
add_executable(ycsb "" db/metakv_db.cc db/metakv_db.h)
target_sources(ycsb PUBLIC
        "core/client.h"
        "core/const_generator.h"
        "core/core_workload.cc"
        "core/core_workload.h"
        "core/counter_generator.h"
        "core/db.h"
        "core/discrete_generator.h"
        "core/generator.h"
        "core/properties.h"
        "core/scrambled_zipfian_generator.h"
        "core/skewed_latest_generator.h"
        "core/timer.h"
        "core/uniform_generator.h"
        "core/utils.h"
        "core/zipfian_generator.h"

        "db/basic_db.h"
        "db/db_factory.cc"
        "db/db_factory.h"
        "db/hashtable_db.cc"
        "db/hashtable_db.h"
        "db/lock_stl_db.h"
        "db/tbb_rand_db.h"
        "db/tbb_scan_db.h"

        "lib/lock_stl_hashtable.h"
        "lib/mem_alloc.h"
        "lib/stl_hashtable.h"
        "lib/string.h"
        "lib/string_hashtable.h"
        "lib/tbb_scan_hashtable.h"
        "lib/tbb_rand_hashtable.h"

        "ycsbc.cc"
        )

if (WITH_PMEM_ROCKSDB)
    message("Define pmem rocksdb")
    add_definitions(-DWITH_DCPMM)
    add_definitions(-DON_DCPMM)
    add_definitions(-DUSING_PMEM_ROCKSDB)
    add_subdirectory(lib/pmem-rocksdb)
    target_include_directories(ycsb PUBLIC lib/pmem-rocksdb/include)
    target_sources(ycsb PUBLIC
            "db/pmem_rocksdb_db.cc"
            "db/pmem_rocksdb_db.h"
            )
    target_link_libraries(ycsb rocksdb)
endif ()

if (WITH_ROART)
	message("Define roart")
	add_definitions(-DUSING_ROART)
        add_definitions(-DCLWB)
        add_definitions(-DVARIABLE_LENGTH)
        add_definitions(-DUSE_PMDK)
        add_definitions(-DACMA) # for fastfair and skiplist with dcmm
        add_definitions(-DFF_GC) # ff_gc
        add_definitions(-DRECLAIM_MEMORY)
        add_definitions(-DKEY_INLINE)
        #add_definitions(-DARTPMDK) # for DLART with PMDK
        #add_definitions(-DCOUNT_ALLOC)
        #add_definitions(-DLOG_FREE)
        #add_definitions(-DCHECK_COUNT)
        add_definitions(-DINSTANT_RESTART)

        add_definitions(-DLEAF_ARRAY)
        add_definitions(-DFIND_FIRST)
        #add_definitions(-DSORT_LEAVES)

        #add_definitions(-DZENTRY)

        #malloc
        #add_definitions(-DPMALLOC)
        #add_definitions(-DTXPMALLOC)
        add_definitions(-DTRANSACTIONAL)
	add_subdirectory(lib/ROART)
	target_include_directories(ycsb PUBLIC lib/ROART/ART
	lib/ROART/benchmark
	lib/ROART/nvm_mgr
	lib/ROART/test
	lib/ROART/fast_fair
	lib/ROART/lf-skiplist)
	target_sources(ycsb PUBLIC
			"db/roart_db.cc"
			"db/roart_db.h"
			)
	target_link_libraries(ycsb Indexes)
endif()

if (WITH_METAKV)
    message("Define MetaKV")
    add_definitions(-DUSING_METAKV)
    add_subdirectory(lib/metakv)
    target_sources(ycsb PUBLIC
            "db/metakv_db.cc"
            "db/metakv_db.h"
            )
    target_include_directories(ycsb PUBLIC lib/metakv .)
    target_link_libraries(ycsb metakv)
endif ()

target_include_directories(ycsb PUBLIC .)
# target_link_libraries(ycsb ${CMAKE_THREAD_LIBS_INIT} ${LIBPMEM_LIBRARIES} ${TBB_LIBRARIES} hiredis)
target_link_libraries(ycsb ${CMAKE_THREAD_LIBS_INIT} ${LIBPMEM_LIBRARIES} ${TBB_LIBRARIES})
