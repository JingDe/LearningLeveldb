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



static const char* FLAGS_benchmarks="WriteSeq,WriteRandom";
static double FLAGS_compression_ratio=0.5;
static int FLAGS_cache_size = -1;
static int FLAGS_num=50; // 写进 数据库 的总的key value 对数
// Size of each value
static int FLAGS_value_size = 100;
// Number of concurrent threads to run.
static int FLAGS_threads = 1;
// Use the db with the following name.
static const char* FLAGS_db = NULL;

namespace leveldb{


Env* g_env = NULL;




// 函数 RandomString 作用： 
// 获得 len长度的随机字符串
Slice RandomString(Random* rnd, int len, std::string* dst) 
{
	dst->resize(len);
	for(int i=0; i<len; i++)
	{
		(*dst)[i]=static_cast<char>(' '+rnd->Uniform(95));
	}
	return Slice(*dst);
}



// 函数CompressibleString 作用：
// 使用 Random类 在dst 中存储len长的随机字符串，该字符串可以压缩到 len*compressed_fraction 字节长
// 返回的Slice 指向 dst 
Slice CompressibleString(Random *rnd, double compressed_fraction, size_t len, std::string* dst)
{
	int raw=static_cast<int>(len *compressed_fraction);
	if(raw<1)
		raw=1;
	std::string raw_data;
	RandomString(rnd, raw, &raw_data); // H
	
	dst->clear();
	while(dst->size()<len)
	{
		dst->append(raw_data);
	}
	dst->resize(len);
	return Slice(*dst);
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
			CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece); // 
			data_.append(piece);
		}
		pos_=0;
	}
	
	Slice Generate(size_t len){
		if(pos_+len > data_.size())
		{
			pos_=0;
			assert(len<data_.size());
		}
		pos_+=len;
		return Slice(data_.data()+pos_-len, len);
	}
};



class ThreadState{
private:
	char* name;
	int ops;
	time_t start;
	int bytes;
	
public:
	ThreadState(char *n):name(n),start(time(NULL)) // epoch后的秒数
	{}
	
	void Update();
	void Report();
};

void ThreadState::Update()
{
	ops++;
	fprintf(stderr, "... finished %dops", ops++);
}

void ThreadState::Report()
{
	time_t end=time(NULL);
	double speed1=(end-start)*1000/op;
	double speed2=bytes/1024/1024/(end-start);
	fprintf(stderr, "%s		: 		%f micros/op; %f MB/s", name, speed1, speed2);
}



struct SharedState{
	pthread_mutex_t mu;
	pthread_cond_t cond;
	int start_num;
	int end_num;
};



struct ThreadArg{
	SharedState* shared;
	ThreadState* thread;
	void (*function)();
};



class Benchmark{
	
private:
	
	//const char *db_name_;
	DB* db_;
	Cache* cache_;

	int bloom_bits_=2;

	WriteOptions write_options_;
	
	int entries_per_batch_;
	int num_;
	int value_size_;
	
	Random rand;
	
	
	void PrintHeader() {
		const int kKeySize = 16;
		PrintEnvironment();
		fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
		fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
				FLAGS_value_size,
				static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
		fprintf(stdout, "Entries:    %d\n", num_);
		fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
				((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_)
				 / 1048576.0));
		fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
				(((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_)
				 / 1048576.0));
		PrintWarnings();
		fprintf(stdout, "------------------------------------------------\n");
	}
	
  void PrintEnvironment() {
    fprintf(stderr, "LevelDB:    version %d.%d\n",
            kMajorVersion, kMinorVersion);

#if defined(__linux)
    time_t now = time(NULL);
    fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != NULL) {
        const char* sep = strchr(line, ':');
        if (sep == NULL) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

  
  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif


  }
  
  
	
	void DoWrite(ThreadState* thread, bool seq)
	{
		write_options_=WriteOptions();
		write_options_.sync=true; // ??
		
		WriteBatch batch;
		RandomGenerator gen;  // 生成 随机 value
		
		entries_per_batch_=10; // 每次batch 写的数目
		
		
		for(int i=0; i<num_; i+= entries_per_batch_)
		{
			batch.Clear();
			for(int j=0; j<entries_per_batch_; j++)
			{
				int k= seq ? i+j : rand.Next() % num_;
				char key[100];
				snprintf(key, sizeof key, "%016d", k);
				
				value_size_=10; // value 大小
				batch.Put(key, gen.Generate(value_size_));
				// 统计 一次Put操作
				thread.Update();
			}
			Status s=db_->Write(write_options_, &batch);
			// 统计 一个batch的Write操作
			if(!s.ok())
			{
				fprintf(stderr, "put error: %s\n", s.ToString().c_str());
				exit(1);
			}
		}
		int nbytes=num_*(sizeof key + value_size_);
		thread.hasWrite(nbytes);
	}
	
	/* void RunBenchmark(int nThreads, void (Benchmark::*method)())
	{
		ThreadState* arg;
		for(int i=0; i<nThreads; i++)
			g_env->StartThread(method, arg);
		//所有线程完成后，打印 arg
		// 问题：g_env->StartThread()只创建线程，保证线程何时结束
	} */

	
	void RunBenchmark(int nThreads, char* name, void (Benchmark::*method)())
	{
		SharedState *shared;
		
		ThreadArg* arg;
		arg.shared=shared;
		arg.thread=new ThreadState(name);
		//arg.thread.name=name;
		arg.function=method;
		for(int i=0; i<nThreads; i++)
			g_env->StartThread(ThreadBody, arg);
		
		
		// ???
		/* shared.mu.lock();
		
		while(shared.start_num < nThreads)
			shared.cond.wait();
		
		while(shared.end_num < nThreads)
			shared.cond.wait();
		
		shared.mu.unlock(); */
		
		arg.thread.Report();
		
	}
	
	void ThreadBody(void *arg)
	{
		ThreadArg* arg=static_cast<ThreadArg*>(arg);
		ThreadState* thread=arg.thread;
		void (*function)()=arg.function;
		
		
		arg.shared.start_num++;
		
		
		function(thread);
		
		arg.shared.end_num++;
	}
  
  
  
public:

	Benchmark()
	: db_(NULL),
	rand(301),
	cache_(FLAGS_cache_size>=0 ? NewLRUCache(FLAGS_cache_size) : NULL)
	num_(FLAGS_num)
	entries_per_batch_(1)
	value_size_(FLAGS_value_size)
	{}
	
	~Benchmark()
	{
		if(db_)
			delete db_;
		if(cache_)
			delete cache_;
	}
	
		
	void Run()
	{
		PrintHeader();
		
		
		Open();
		
		const char *benchmarks=FLAGS_benchmarks;
		const char* sep;
		std::string name;
		while(benchmarks)
		{
			sep=strchr(benchmarks, ',');
			if(sep==NULL)
			{
				name=benchmarks;
				benchmarks=NULL;
			}
			else
			{
				
				name=std::string(benchmarks, sep-benchmarks);
				
				benchmarks=sep+1;
			}
			
			
			int num_threads=FLAGS_threads;
			void (Benchmark::*method)()=NULL;
			
			if(name=="WriteSeq")
				method=&Benchmark::WriteSeq;
			else if(name=="WriteRandom")
				method=&Benchmark::WriteRandom;
			else if (name == Slice("fillsync")) 
			{
				num_ /= 1000;
				write_options_.sync = true;
				method = &Benchmark::WriteRandom;
			} else if (name == Slice("fill100K")) 
			{
				num_ /= 1000;
				value_size_ = 100 * 1000;
				method = &Benchmark::WriteRandom;
			}
			
			if(method)
			{
				(this->*method)();
			// 创建多个线程 执行method，
			// 等待每个线程的启动 和完成，执行完成后 汇总并打印状态
			// 
				RunBenchmark(num_threads, name, method);
			}
		}
				
	}

	
	
	void Open()
	{
		assert(db_==NULL);
		Options options;
		options.env=g_env;
		options.create_if_missing=true;
		// options.error_if_exists = true;
		options.block_cache=cache_;
		options.write_buffer_size=leveldb::Options().write_buffer_size;
		options.max_file_size=leveldb::Options().max_file_size;
		options.block_size=leveldb::Options().block_size;
		options.max_open_files=leveldb::Options().max_open_files;
		options.filter_policy= NewBloomFilterPolicy(bloom_bits_);
		options.reuse_logs=false; // ???
		
		
		Status s;
		s=DB::Open(options, FLAGS_db, &db_); // 在当前目录CreateDir
		//s=DB::Open(options, db_name_, &db_);
		if(!s.ok())
		{
			fprintf(stderr, "open error: %s\n", s.ToString().c_str());
			exit(1);
		} 
		
	}


	void WriteSeq(ThreadState* thread)
	{
		DoWrite(thread, true);
	}
	
	void WriteRandom(ThreadState* thread)
	{
		DoWrite(thread, false);
	}
	
	


};

}

int main(int argc, char** argv)
{
	std::string  default_db_path;
	
	for(int i=0; i<argc; i++)
	{
		int n;
		char* c;
		/* if(std::string(argv[i])=="--benchmarks"  &&  i<argc-1)
		{
			FLAGS_benchmarks=argv[i+1];
		} */
		if(std::string(argv[i]).starts_with("--benchmarks="))
		{
			FLAGS_benchmarks=argv[i]+strlen("--benchmarks=");
		} 
		else if(sscanf(argv[i], "--cache_size=%d%s", &n, c)==1)
		{
			FLAGS_cache_size=n;
		}
		else if(sscanf(argv[i], "--compression_ratio=%d%s", &n, c)==1)
		{
			FLAGS_compression_ratio=n;
		}
		else if(sscanf(argv[i], "--num=%d%s", &n, c)==1)
		{
			FLAGS_num=n;
		}
		else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
		  FLAGS_threads = n;
		}
	}
	
	leveldb::g_env=leveldb::Env::Default();
	
	// 指定 数据库文件所在目录
	if(FLAGS_db==NULL)
	{
		leveldb::g_env->GetTestDirectory(&default_db_path);
		default_db_path+="/dbbench";
		FLAGS_db=default_db_path.c_str();
	}
	fprintf(stderr, "FLAGS_db = %s\n", FLAGS_db);
	
	leveldb::Benchmark bm;
	bm.Run();
	//bm.Open();
	
	return 0;
}