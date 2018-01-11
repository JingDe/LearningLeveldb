#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "env.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include<sys/time.h>


static const char* FLAGS_benchmarks="fillseq";
static double FLAGS_compression_ratio=0.5;
static int FLAGS_cache_size = -1;

// 写进 数据库 的总的key value 对数
static int FLAGS_num=1000000; 

// Size of each value
static int FLAGS_value_size = 100;
// Number of concurrent threads to run.
static int FLAGS_threads = 1;
// Use the db with the following name.
static const char* FLAGS_db = NULL;
// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// If true, reuse existing log/MANIFEST files when re-opening a database.
static bool FLAGS_reuse_logs = false;

// Number of bytes written to each file.
// (initialized to default value by "main")
static int FLAGS_max_file_size = 0;

// Approximate size of user data packed per block (before compression.
// (initialized to default value by "main")
static int FLAGS_block_size = 0;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Print histogram of operation timings
static bool FLAGS_histogram = false;



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


#if defined(__linux)
static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit-1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}
#endif



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


int64_t microSeconds()
{
	struct timeval tm;
	gettimeofday(&tm, NULL);
	return static_cast<uint64_t>(tm.tv_sec*1000000 + tm.tv_usec);
}



class Stats{
private:
	int64_t start_; // 微秒
	int64_t finish_;
	int next_report_;
	int ops_;
	int bytes_;
	
public:
	Stats()
	{
		Start();
	}
	void Start();
	void Merge(const Stats& other);
	void Stop()
	{
		finish_=microSeconds();
	}
	void FinishedSingleOp();
	void AddBytes(int n)
	{ bytes_+=n; }
	void Report(const std::string& name);

};

void Stats::Start()
{
	start_=microSeconds();
	finish_=start_;
	next_report_=10;
	ops_=0;
	bytes_=0;
}

void Stats::Merge(const Stats& other)
{
	ops_+=other.ops_;
	bytes_+=other.bytes_;
	if(other.start_ < start_) start_=other.start_;
	if(other.finish_ > finish_) finish_=other.finish_;
}

void Stats::FinishedSingleOp()
{
	ops_++;
	if(ops_>=next_report_)
	{	
		if      (next_report_ < 100)    next_report_ += 10;
		else if (next_report_ < 1000)   next_report_ += 100;
        else if (next_report_ < 5000)   next_report_ += 500;
        else if (next_report_ < 10000)  next_report_ += 1000;
        else if (next_report_ < 50000)  next_report_ += 5000;
        else if (next_report_ < 100000) next_report_ += 10000;
        else if (next_report_ < 500000) next_report_ += 50000;
        else                            next_report_ += 100000;
		fprintf(stderr, "... finished %d ops\r", ops_);
		fflush(stderr);
	}
}

void Stats::Report(const std::string& name)
{
	uint64_t elapsed=finish_-start_;//微秒
	fprintf(stdout, "microSeconds=%lld, ops=%ld, bytes=%d\n", elapsed, ops_, bytes_);
	double speed1=elapsed/ops_;
	
	double speed2=((bytes_)/(1024*1024))/(elapsed/1000000);
	fprintf(stdout, "%-12s	   :    %11.3f micros/op;   %6.1f MB/s\n", name.c_str(), speed1, speed2);
} 



struct ThreadState{
	//std::string name;
	int tid;
	Random rand; //不同线程有不同的Random
	Stats stats;
	
	ThreadState(int index)
		:tid(index),
		rand(1000+index),
		stats(Stats())
	{}
};




class SharedState{
public:
	pthread_mutex_t mu;
	pthread_cond_t cond;
	int total;
	int start_num;
	int end_num;
	bool start;
	
	SharedState(int t)
	{
		total=t;
		start_num=0;
		end_num=0;
		start=false;
		pthread_mutex_init(&mu, NULL);
		pthread_cond_init(&cond, NULL);
	}
	~SharedState()
	{
		pthread_mutex_destroy(&mu);
		pthread_cond_destroy(&cond);
	}
};







class Benchmark{
	
private:
	
	//const char *db_name_;
	DB* db_;
	Cache* cache_;
	const FilterPolicy* filter_policy_;
	int value_size_;
	int entries_per_batch_;
	int num_;
		
	WriteOptions write_options_;
	
	//Random rand; 每个执行操作的线程拥有一个不同的Random
	
	
	
	struct ThreadArg{
		SharedState* shared;
		ThreadState* thread;
		Benchmark *bm;
		void (Benchmark::*method)(ThreadState*);
	};


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
		WriteBatch batch;
		RandomGenerator gen;  // 生成 随机 value
		Status s;
		int64_t nbytes=0;
				
		for(int i=0; i<num_; i+= entries_per_batch_)
		{
			batch.Clear();
			for(int j=0; j<entries_per_batch_; j++)
			{
				const int k= seq ? i+j : (thread->rand.Next() % FLAGS_num);
				char key[100];
				snprintf(key, sizeof key, "%016d", k);
				
				batch.Put(key, gen.Generate(value_size_));
				
				nbytes+= strlen(key) + value_size_;
				thread->stats.FinishedSingleOp();
			}
											
			s=db_->Write(write_options_, &batch);
						
			if(!s.ok())
			{
				fprintf(stderr, "put error: %s\n", s.ToString().c_str());
				exit(1);
			}
		}
		//int nbytes=num_*(sizeof key + value_size_);
		thread->stats.AddBytes(nbytes);
	}
	
	/* void RunBenchmark(int nThreads, void (Benchmark::*method)())
	{
		ThreadState* arg;
		for(int i=0; i<nThreads; i++)
			g_env->StartThread(method, arg);
		//所有线程完成后，打印 arg
		// 问题：g_env->StartThread()只创建线程，保证线程何时结束
	} */

	
	void RunBenchmark(int nThreads, std::string name, void (Benchmark::*method)(ThreadState*))
	{
		SharedState shared(nThreads);
		
		/*ThreadArg arg; 
		arg.bm=this;
		arg.shared=&shared;
		arg.thread=new ThreadState(name);
		//arg.thread.name=name;
		arg.method=method;
		
		for(int i=0; i<nThreads; i++)
			g_env->StartThread(ThreadBody, &arg);
		*/
		
		ThreadArg arg[nThreads];//为什么这里要创建数组？？
//应为ThreadArg.ThreadState.Stats的bytes_、ops_等统计数据为int而非AtomicInt，不同线程不能不加锁访问
		for(int i=0; i<nThreads; i++)
		{
			arg[i].bm=this;
			arg[i].method=method;
			arg[i].shared=&shared;
			arg[i].thread=new ThreadState(i); // 因为每个线程一个ThreadState
			g_env->StartThread(ThreadBody, &arg[i]);
		}
		
		
		
		pthread_mutex_lock(&shared.mu);
		
		while(shared.start_num < nThreads)
			pthread_cond_wait(&shared.cond, &shared.mu);
		
		shared.start=true;
		pthread_cond_broadcast(&shared.cond);
				
		
		
		while(shared.end_num < nThreads)
			pthread_cond_wait(&shared.cond, &shared.mu);
		
		pthread_mutex_unlock(&shared.mu);
		
			
		/*arg.thread->stats.Report(name); 	
		delete arg.thread;*/
		
		for(int i=1; i<nThreads; i++)
		{
			arg[0].thread->stats.Merge(arg[i].thread->stats); //最后Merge所有的ThreadState
		}
		arg[0].thread->stats.Report(name);
		
		
		for(int i=0; i<nThreads; i++)
			delete arg[i].thread;
		//delete[] arg;		
	}
	
	static void ThreadBody(void *a)
	{
		ThreadArg* arg=static_cast<ThreadArg*>(a);
		ThreadState* thread=arg->thread;
		SharedState* shared=arg->shared;
		
		pthread_mutex_lock(&shared->mu);
		shared->start_num++;
		if(shared->start_num >= shared->total)
			pthread_cond_broadcast(&shared->cond);
		while(shared->start==false)
			pthread_cond_wait(&shared->cond, &shared->mu);
		pthread_mutex_unlock(&shared->mu);
			
		
		thread->stats.Start();//计时更准确
		(arg->bm->*(arg->method))(thread);
		thread->stats.Stop();
		
		
		pthread_mutex_lock(&shared->mu);
		shared->end_num++;
		if(shared->end_num>=shared->total)
			pthread_cond_broadcast(&shared->cond);
		pthread_mutex_unlock(&shared->mu);
	}
  
  
  
public:

	Benchmark()
	: db_(NULL),	
	cache_(FLAGS_cache_size>=0 ? NewLRUCache(FLAGS_cache_size) : NULL),
	filter_policy_(FLAGS_bloom_bits >= 0
                   ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                   : NULL),
	value_size_(FLAGS_value_size),
	entries_per_batch_(1),
	num_(FLAGS_num),
	write_options_(WriteOptions())
	{
		//删除已存在的数据库文件，删除数据库
		std::vector<std::string> files;
		//g_env->GetChildren(FLAGS_db, &files);
		MyGetChildren(FLAGS_db, &files);
		for(size_t i=0; i<files.size(); i++)
		{
			if(strcmp(files[i].c_str(), "heap-")==0)
				MyDeleteFile(std::string(FLAGS_db)+"/"+files[i]);
				//g_env->DeleteFile(std::string(FLAGS_db) + "/" +files[i]);
		}
		if (!FLAGS_use_existing_db) {
		  DestroyDB(FLAGS_db, Options());
		}
	}
	
	~Benchmark()
	{
		if(db_)
			delete db_;
		if(cache_)
			delete cache_;
		if(filter_policy_)
			delete filter_policy_;
	}
	
	void ClearDB()
	{
		//删除已存在的数据库文件，删除数据库
		std::vector<std::string> files;
		//g_env->GetChildren(FLAGS_db, &files);
		MyGetChildren(FLAGS_db, &files);
		for(size_t i=0; i<files.size(); i++)
		{
			if(strcmp(files[i].c_str(), "heap-")==0)
				MyDeleteFile(std::string(FLAGS_db)+"/"+files[i]);
				//g_env->DeleteFile(std::string(FLAGS_db) + "/" +files[i]);
		}
		if (!FLAGS_use_existing_db) {
		  DestroyDB(FLAGS_db, Options());
		}
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
			void (Benchmark::*method)(ThreadState*)=NULL;
			
			if(name=="fillseq")
				method=&Benchmark::WriteSeq;
			else if(name=="fillrandom")
				method=&Benchmark::WriteRandom;
			else if (name == "fillsync")
			{
				num_ /= 1000;
				write_options_.sync = true;
				method = &Benchmark::WriteRandom;
			} else if (name == "fill100K")
			{
				num_ /= 1000;
				value_size_ = 100 * 1000;
				method = &Benchmark::WriteRandom;
			}
			else if (name == "fillbatch") {
				entries_per_batch_ = 1000;
				method = &Benchmark::WriteSeq;
			}
			
			if(method)
			{
				//(this->*method)();
			// 创建多个线程 执行method，
			// 等待每个线程的启动 和完成，执行完成后 汇总并打印状态
			// 
				RunBenchmark(num_threads, name, method);
			}
		}
		ClearDB();
	}

	
	
	void Open()
	{
		assert(db_==NULL);
		Options options;
		options.env=g_env;
		options.create_if_missing=!FLAGS_use_existing_db;
		// options.error_if_exists = true;
		options.block_cache=cache_;
		options.write_buffer_size=FLAGS_write_buffer_size;
		options.max_file_size=FLAGS_max_file_size;
		options.block_size=FLAGS_block_size;
		options.max_open_files=FLAGS_open_files;
		options.filter_policy= filter_policy_;
		options.reuse_logs=FLAGS_reuse_logs; 
					
 						
		Status s=DB::Open(options, FLAGS_db, &db_);
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

void myGetTestDirectory(std::string* path)
{
	*path=".";
	//*path="/home/wangjing/workspace/leveldb_study/leveldb-master/mytests";
}

int main(int argc, char** argv)
{	
	FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
	FLAGS_max_file_size = leveldb::Options().max_file_size;
	FLAGS_block_size = leveldb::Options().block_size;
	FLAGS_open_files = leveldb::Options().max_open_files;
  
	std::string  default_db_path;
	
	for(int i=0; i<argc; i++)
	{
		int n;
		char c;
		/* if(std::string(argv[i])=="--benchmarks"  &&  i<argc-1)
		{
			FLAGS_benchmarks=argv[i+1];
		} */
		//if(std::string(argv[i]).starts_with("--benchmarks="))
		if(memcmp(argv[i], "--benchmarks=", strlen("--benchmarks="))==0)
		{
			FLAGS_benchmarks=argv[i]+strlen("--benchmarks=");
		} 
		else if(sscanf(argv[i], "--cache_size=%d%s", &n, &c)==1)
		{
			FLAGS_cache_size=n;
		}
		else if(sscanf(argv[i], "--compression_ratio=%d%s", &n, &c)==1)
		{
			FLAGS_compression_ratio=n;
		}
		else if(sscanf(argv[i], "--num=%d%s", &n, &c)==1)
		{
			FLAGS_num=n;
		}
		else if (sscanf(argv[i], "--threads=%d%c", &n, &c) == 1) {
		  FLAGS_threads = n;
		}
		else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &c) == 1) {
		  FLAGS_bloom_bits = n;
		}
		else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &c) == 1  &&  (n == 0 || n == 1)) {
		  FLAGS_use_existing_db = n;
		}
		else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &c) == 1) {
		  FLAGS_write_buffer_size = n;
		}
		else if (sscanf(argv[i], "--reuse_logs=%d%c", &n, &c) == 1 &&
               (n == 0 || n == 1)) {
		  FLAGS_reuse_logs = n;
		}
		else if (sscanf(argv[i], "--value_size=%d%c", &n, &c) == 1) {
		  FLAGS_value_size = n;
		}
		else if (sscanf(argv[i], "--max_file_size=%d%c", &n, &c) == 1) {
		  FLAGS_max_file_size = n;
		}
		else if (sscanf(argv[i], "--block_size=%d%c", &n, &c) == 1) {
		  FLAGS_block_size = n;
		}
		else if (sscanf(argv[i], "--block_size=%d%c", &n, &c) == 1) {
		  FLAGS_block_size = n;
		}
		else if (sscanf(argv[i], "--open_files=%d%c", &n, &c) == 1) {
		  FLAGS_open_files = n;
		}
		else if (sscanf(argv[i], "--histogram=%d%c", &n, &c) == 1 &&
               (n == 0 || n == 1)) {
		  FLAGS_histogram = n;
		}
	}
	
	leveldb::g_env=leveldb::Env::Default();
	
	// 指定 数据库文件所在目录
	if(FLAGS_db==NULL)
	{
		leveldb::g_env->GetTestDirectory(&default_db_path);
		//myGetTestDirectory(&default_db_path);
		default_db_path+="/mydbbench";
		FLAGS_db=default_db_path.c_str();
	}
	fprintf(stderr, "FLAGS_db = %s\n", FLAGS_db);
	
	leveldb::Benchmark bm;
	bm.Run();
	
	
	return 0;
}