#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"


// using namespace leveldb;
namespace leveldb{


Env* g_env = NULL;
double Flags_compression_ratio=0.5;


// 函数CompressibleString 作用：
// 使用 Random类 在dst 中存储len长的随机字符串，该字符串可以压缩到 len*compressed_fraction 字节长
// 返回的Slice 指向 dst 
Slice CompressibleString(Random *rnd, double compressed_fraction, size_t len, std::string* dst)
{
	int raw=static_cast<int>(len *compressed_fraction);
	if(raw<1)
		raw=1;
	std::string raw_data;
	RandomString(rnd, raw, &raw_data);
	
	
}


// 类RandomGenerator作用：
// 预先生成一个足够长 1048576 的随机数组，用来快速获得指定长度的随机数组
class RandomGenerator{
private:
	std::string data_;
	int pos_;
	
public:
	RandomGenerator(){
		Random rnd(301);  // Random类
		std::string piece;
		while(data_.size() < 1048576)
		{
			CompressibleString(&rnd, Flags_compression_ratio, 100, &piece); // 
			data_.append(piece);
		}
		pos_=0;
	}
};






class BenchMark{
	
private:
	
	const char *db_name="MyDB01";
	DB* db;
	Cache* cache;

	int bloom_bits=2;

	WriteOptions write_options_;
	int entries_per_batch_;
	int nums_; // 写进 数据库 的总的key value 对数
	
	int value_size_;

public:

	void Opendb()
	{
		assert(db==NULL);
		Options options;
		options.env=g_env;
		options.create_if_missing=true;
		// options.error_if_exists = true;
		options.block_cache=cache;
		options.write_buffer_size=leveldb::Options().write_buffer_size;
		options.max_file_size=leveldb::Options().max_file_size;
		options.block_size=leveldb::Options().block_size;
		options.max_open_files=leveldb::Options().max_open_files;
		options.filter_policy= NewBloomFilterPolicy(bloom_bits);
		options.reuse_logs=true; // ???
		
		
		Status s=DB::Open(options, 	db_name, &db);
		if(!s.ok())
		{
			fprintf(stderr, "open error: %s\n", s.ToString().c_str());
			exit(1);
		}
		
	}


	void Write()
	{
		write_options_=WriteOptions();
		write_options_.sync=true; // ??
		
		WriteBatch batch;
		RandomGenerator gen;  // 生成 随机 value
		
		entries_per_batch_=10; // 每次batch 写的数目
		nums_=100; 
		
		for(int i=0; i<nums_; i+= entries_per_batch_)
		{
			batch.clear();
			for(int j=0; j<entries_per_batch_; j++)
			{
				char key[100]="1234567890";  // 生成随机 key
				value_size_=10; // value 大小
				// Slice key;
				Slice value;
				batch.put(key, value);
				
			}
			Status s=db_->Write(write_options_, &batch);
			if(!s.ok())
			{
				fprintf(stderr, "put error: %s\n", s.ToString().c_str());
				exit(1);
			}
		}
	}

}

}

int main(int argc, char** argv)
{
	leveldb::g_env=leveldb::Env::Default();
	
	leveldb::Opendb();
	
	
	return 0;
}