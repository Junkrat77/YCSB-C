#include "roart_db.h"
#include "lib/ROART/nvm_mgr/threadinfo.h"

using namespace ycsbc;

namespace ycsbc_roart {
	// const std::string PMEM_PATH("/mnt/pmem/");
    // const std::string DB_NAME("/mnt/pmem/rocksdb/");
    // const uint64_t PMEM_SIZE = 60 * 1024UL * 1024UL * 1024UL;
	void RoartDB::Init() {
		printf("test\n");
		art = new PART_ns::Tree();
		// TODO: need init?
	}

	void RoartDB::Close() {
		delete art;
		// db_->Close();
        // delete db_;
        // db_ = nullptr;
	}

	int RoartDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values){
		std::string k = table + key;
        std::string v;
        for (const auto& item : values) {
            k.append(item.first + item.second);
        }
		
		bool res = art->Put(k, v);
		// printf("test\n");
		if (res == true) {
			return DB::kOK;
		} else {
			fprintf(stderr, "[%s] already existed\n", k.c_str());
            return DB::kOK;
		}
	}

	int RoartDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                          std::vector<KVPair> &result) {
		std::string k = table + key;
		// std::string raw_value;
		const std::string* v;
		bool ret = art->Get(k, v);
		if (ret == 0) {
			return DB::kErrorNoData;
		}
		return DB::kOK;
	}


	int RoartDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
		std::string whole_key = table + key;
        std::string whole_value;
        for (const auto& item : values) {
            whole_value.append(item.first + item.second);
        }
		PART_ns::Key k;
		k.Init((char*)whole_key.c_str(), whole_key.size(), (char*)whole_value.c_str(), whole_value.size());
		PART_ns::Tree::OperationResults res = art->update(&k);
		if (res == PART_ns::Tree::OperationResults::Success) {
			return DB::kOK;
		} else { // NotFound
			fprintf(stderr, "[%s] already existed\n", whole_key.c_str());
            return DB::kErrorNoData;
		}
	}

	int RoartDB::Delete(const std::string &table, const std::string &key) {
		std::string k = table + key;
		bool res = art->Delete(k);
		if (1 == res) {
			return DB::kOK;
		} else {
			// not found
			return DB::kErrorNoData;
		}
	}

    int RoartDB::Scan(const std::string &table, const std::string &key, int record_count,
                          const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
		std::string k = table + key;
		std::vector<KVPair> v;
		art->Scan(k ,v);
		// printf("scan key      = %s, scan_length = %d\n", whole_key.c_str(), record_count);
		// for (int i = 0; i < resultFound; i++) {
			// printf("%s\n", scan_result[i]->GetKey());
		// } 	
		return DB::kOK;
	}

    void register_threadinfo() {
		NVMMgr_ns::register_threadinfo();
	}
}
