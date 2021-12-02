//
// Created by naupn on 2021/11/29.
//

#include "metakv_db.h"
#include <thread>

namespace ycsb_metakv{
    void ycsbMetaKV::Init() {
        struct DBOption db_option;
        SetDefaultDBop(&db_option);
//#ifdef DMETAKV_CACHE
//        SetCacheOp(1, 1, (100ul * (1ul << 20)), (100ul * (1ul << 20)))
//#endif
        DBOpen(&db_option,"/home/ganquan/metakv", &db);
    }

    void ycsbMetaKV::Close() {
        DBExit(&db);
    }

    int ycsbMetaKV::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        // 完全不管table
        const std::string& whole_key(key);
        std::string whole_value;

        if (values.size() != 1) {
            printf("Insert error: values size should be exactly 1");
            exit(0);
        }

        uint64_t pinode = std::stoull(whole_key.substr(0, whole_key.find('-')));
        std::string tmp_fname = whole_key.substr(whole_key.find('-')+1,  whole_key.size()-1);
        struct Slice fname;
        SliceInit(&fname, tmp_fname.size() + 1, tmp_fname.data());
        uint64_t inode = std::stoull(values.at(0).first);
        std::string stat_str = values.at(0).second;
        printf("[thread: %lu] INSERT: pinode: %lu, fname: %s, inode: %lu\n", std::this_thread::get_id(), pinode, tmp_fname.c_str(), inode);
        fflush(stdout);
        if (MetaKVPut(&db, pinode, &fname, inode, reinterpret_cast<struct stat *>(stat_str.data())) != 0) {
            return DB::kErrorNoData;
        }

        return DB::kOK;
    }

    int ycsbMetaKV::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                         std::vector<KVPair> &result) {
        const std::string& whole_key(key);
        std::string whole_value;

        uint64_t pinode = std::stoull(whole_key.substr(0, whole_key.find('-')));
        std::string tmp_fname = whole_key.substr(whole_key.find('-')+1,  whole_key.size()-1);
        struct Slice fname;
        SliceInit(&fname, tmp_fname.size() + 1, tmp_fname.data());
        uint64_t inode = 0;
        struct Slice stat_slice;

        printf("GET: pinode: %lu, fname: %s\n", pinode, tmp_fname.c_str());
        fflush(stdout);

        if (MetaKVGet(&db, pinode, &fname, &inode, &stat_slice) != 1) {
            return DB::kErrorNoData;
        }
        return DB::kOK;
    }

    int ycsbMetaKV::Delete(const std::string &table, const std::string &key) {
        const std::string& whole_key(key);
        std::string whole_value;

        uint64_t pinode = std::stoull(whole_key.substr(0, whole_key.find('-')));
        std::string tmp_fname = whole_key.substr(whole_key.find('-')+1,  whole_key.size()-1);
        struct Slice fname;
        SliceInit(&fname, tmp_fname.size() + 1, tmp_fname.data());
        if (MetaKVDelete(&db, pinode, &fname) != 0) {
            return DB::kErrorNoData;
        }
        return DB::kOK;
    }

    int ycsbMetaKV::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        // for workload a/b/c/d/e/f should use insert instead of delete
        return Insert(table, key, values);
    }

    int ycsbMetaKV::Scan(const std::string &table, const std::string &key, int record_count,
                         const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        const std::string& whole_key(key);
        std::string whole_value;

        uint64_t pinode = std::stoull(whole_key.substr(0, whole_key.find('-')));
        std::string tmp_fname = whole_key.substr(whole_key.find('-')+1,  whole_key.size()-1);
        struct Slice fname;
        SliceInit(&fname, tmp_fname.size() + 1, tmp_fname.data());
        char * r = nullptr;
        if (MetaKVScan(&db, pinode, &r) != 0 || r == nullptr) {
            return DB::kErrorNoData;
        }
        return DB::kOK;
    }
}
