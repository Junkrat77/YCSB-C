cmake_minimum_required(VERSION 3.10)
project(ycsb)

set(CMAKE_CXX_STANDARD 17)

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g")
include(FindThreads)
find_package(PkgConfig REQUIRED)
pkg_check_modules(TBB REQUIRED tbb)
pkg_check_modules(LIBPMEM REQUIRED libpmem)
pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)

# add_subdirectory(redis)

# only on DB can be set to ON when building ycsb
#as some function name may conflict
option(WITH_PMEM_ROCKSDB "support the pmem rocksdb" ON)
option(WITH_CCEH "support for CCEH hash table" OFF)
option(WITH_UTREE "support for uTree" OFF)
option(WITH_VIPER "support for Viper" OFF)
option(WITH_HIKV "support for HiKV" OFF)
option(WITH_METAKV "support for MetaKV" OFF)
option(WITH_ROART "support for Roart" OFF)

add_executable(ycsb "")
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
        #  "db/redis_db.cc"
        #  "db/redis_db.h"
        "db/tbb_rand_db.h"
        "db/tbb_scan_db.h"
        "db/metadb.cc"
        "db/metadb.h"

        "lib/lock_stl_hashtable.h"
        "lib/mem_alloc.h"
        "lib/stl_hashtable.h"
        "lib/string.h"
        "lib/string_hashtable.h"
        "lib/tbb_scan_hashtable.h"
        "lib/tbb_rand_hashtable.h"
        #"lib/pm_log_store.h"

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

if (WITH_CCEH)
    message("Define cceh")
    add_definitions(-DUSING_CCEH)
    add_subdirectory(lib/cceh)
    target_include_directories(ycsb PUBLIC lib/cceh/CCEH-PMDK/src ${LIBPMEMOBJ_INCLUDE_DIRS})
    target_sources(ycsb PUBLIC
            "db/cceh_db.cc"
            "db/cceh_db.h"
            )
    target_link_libraries(ycsb cceh ${LIBPMEMOBJ_LIBRARIES})
endif ()

if (WITH_UTREE)
    message("Define utree")
    add_definitions(-DUSING_UTREE)
    #add_definitions(-DUSE_PMDK)
    add_subdirectory(lib/utree)
    target_include_directories(ycsb PUBLIC lib/utree/multiThread/utree ${LIBPMEMOBJ_INCLUDE_DIRS})
    target_sources(ycsb PUBLIC
            "db/utree_db.cc"
            "db/utree_db.h"
            )
    target_link_libraries(ycsb utree ${LIBPMEMOBJ_LIBRARIES})
endif ()

if (WITH_VIPER)
    message("Define viper")
    add_definitions(-DUSING_VIPER)
    add_subdirectory(lib/viper)
    target_sources(ycsb PUBLIC
            "db/viper_db.cc"
            "db/viper_db.h"
            )
    target_include_directories(ycsb PUBLIC lib/viper/include .)
    target_link_libraries(ycsb viper stdc++fs)
endif ()

if (WITH_HIKV)
    message("Define hikv")
    add_definitions(-DUSING_HIKV)
    add_subdirectory(lib/hikv)
    target_sources(ycsb PUBLIC
            "db/hikvdb.cc"
            "db/hikvdb.h"
            )
    target_include_directories(ycsb PUBLIC lib/hikv .)
    target_link_libraries(ycsb open_hikv)
endif ()

if (WITH_METAKV)
    message("Define MetaKV")
    add_definitions(-DUSING_METAKV)
    add_subdirectory(lib/metakv)
    target_sources(ycsb PUBLIC
            "db/metakv_db.cc"
            "db/metakv_db.h"
            )
    link_libraries(lib/metakv/src/CLHT)
    find_library(CLHT_LIBRARIES NAMES clht PATHS lib/metakv/src/CLHT REQUIRED)
    find_library(SSMEM_LIBRARIES NAMES ssmem PATHS lib/metakv/src/CLHT REQUIRED)
    target_include_directories(ycsb PUBLIC lib/metakv .)
    target_link_libraries(ycsb metakv)
endif ()

target_include_directories(ycsb PUBLIC .)
# target_link_libraries(ycsb ${CMAKE_THREAD_LIBS_INIT} ${LIBPMEM_LIBRARIES} ${TBB_LIBRARIES} hiredis)
target_link_libraries(ycsb ${CMAKE_THREAD_LIBS_INIT} ${LIBPMEM_LIBRARIES} ${TBB_LIBRARIES})
