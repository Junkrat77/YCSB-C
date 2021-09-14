//
// Created by zzyyywwcd shuaweibo  on 2021/8/23.
//

#ifndef YCSB_PMEM_ROCKSDB_DB_H
#define YCSB_PMEM_ROCKSDB_DB_H

#include <string>
#include <vector>

#include "core/db.h"
#include "lib/pmem-rocksdb/include/rocksdb/db.h"

namespace ycsb_pmem_rocksdb {
class PmemRocksDB : public ycsbc::DB {
    public:
        PmemRocksDB(){}

        ~PmemRocksDB(){}

        void Init();

        void Close();

        int Read(const std::string &table, const std::string &key,
                         const std::vector<std::string> *fields,
                         std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                         int record_count, const std::vector<std::string> *fields,
                         std::vector<std::vector<KVPair>> &result);

        int Update(const std::string &table, const std::string &key,
                           std::vector<KVPair> &values);

        int Insert(const std::string &table, const std::string &key,
                           std::vector<KVPair> &values);

        int Delete(const std::string &table, const std::string &key);

    private:
        rocksdb::DB* db_;
    };
}


#endif //YCSB_PMEM_ROCKSDB_DB_H
