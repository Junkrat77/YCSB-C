#include "roart_db.h"
#include "lib/ROART/nvm_mgr/threadinfo.h"

using namespace ycsbc;

namespace ycsbc_roart {
	void register_threadinfo() {
		NVMMgr_ns::register_threadinfo();
	}
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
		std::string whole_key = table + key;
        std::string whole_value;
        for (const auto& item : values) {
            whole_value.append(item.first + item.second);
        }
		PART_ns::Key k;
		k.Init((char*)whole_key.c_str(), whole_key.size(), (char*)whole_value.c_str(), whole_value.size());
		// printf("test\n");
		PART_ns::Tree::OperationResults res = art->insert(&k);
		// printf("test\n");
		if (res == PART_ns::Tree::OperationResults::Success) {
			return DB::kOK;
		} else {
			fprintf(stderr, "[%s] already existed\n", whole_key.c_str());
            return DB::kOK;
		}
	}

	int RoartDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                          std::vector<KVPair> &result) {
		std::string whole_key = table + key;
		// std::string raw_value;
		PART_ns::Key k;
		uint64_t value = 0;
		k.Init((char*)whole_key.c_str(), whole_key.size(), (char*)&value, 8);
		PART_ns::Leaf* ret = art->lookup(&k);
		if (ret == nullptr) {
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
		std::string whole_key = table + key;
		PART_ns::Key k;
		uint64_t value = 0;
		k.Init((char*)whole_key.c_str(), whole_key.size(), (char*)&value, 8);
		PART_ns::Tree::OperationResults res = art->remove(&k);
		if (PART_ns::Tree::OperationResults::Success == res) {
			return DB::kOK;
		} else {
			// not found
			return DB::kErrorNoData;
		}
	}

    int RoartDB::Scan(const std::string &table, const std::string &key, int record_count,
                          const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        PART_ns::Key k, maxkey, biggerkey;
		uint64_t value = 0;
		std::string whole_key = table + key;
		std::string prefix = whole_key.substr(0, 20);
                k.Init((char*)prefix.c_str(), prefix.size(), (char*)&value, 8);
		PART_ns::Key getkey;
		getkey.Init((char*)whole_key.c_str(), whole_key.size(), (char*)&value, 8);
        maxkey.Init((char*)"z", 1, (char*)&value, 8);
		size_t resultFound = 0;
		PART_ns::Key* continueKey = nullptr;
        PART_ns::Leaf *scan_result[1005];
		// printf("string prefix = %s\n", prefix.c_str());
		// record_count = 10;
		// PART_ns::Leaf* get_res = art->lookup(&getkey);
		uint64_t prefix_len = 15;
		std::string bigger = whole_key.substr(0, prefix_len);
		bigger[prefix_len-1] = (char)((char)bigger[prefix_len-1]+1);
biggerkey.Init((char*)bigger.c_str(), bigger.size(), (char*)&value, 8);
		// printf("bigger prefix = %s\n", bigger.c_str());
		art->lookupRange(&getkey, &biggerkey, continueKey, scan_result, record_count, resultFound);
		// printf("scan key      = %s, scan_length = %d\n", whole_key.c_str(), record_count);
		// for (int i = 0; i < resultFound; i++) {
			// printf("%s\n", scan_result[i]->GetKey());
		// } 	
		return DB::kOK;
	}
<<<<<<< HEAD
}
=======

    void register_threadinfo() {
		NVMMgr_ns::register_threadinfo();
	}
}
>>>>>>> 20546db465fb90e9376baa7afecec6375a868c34
