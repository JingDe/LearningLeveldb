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
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include<sys/time.h>
#include"histogram.h"
#include"atomic_pointer.h"


static const char* FLAGS_benchmarks="fillseq,"
    "fillsync,"
    "fillrandom,"
    "overwrite,"
    "readrandom,"
    "readrandom,"  // Extra run to allow previous compactions to quiesce
    "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,"
    "acquireload,"
    ;
	
static double FLAGS_compression_ratio=0.5;
static int FLAGS_cache_size = -1;

// 写进 数据库 的总的key value 对数
static int FLAGS_num=1000000; 

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

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

static void AppendWithSpace(std::string* str, Slice msg) {
	if (msg.empty()) return;
	if (!str->empty()) {
		str->push_back(' ');
	}
	str->append(msg.data(), msg.size());
}

class Stats{
private:
	int64_t start_; // 微秒
	int64_t finish_;
	int next_report_;
	int ops_;
	int64_t bytes_;
	double last_op_finish_;
	Histogram hist_;
	std::string message_;
	
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
	void AddBytes(int64_t n)
	{ bytes_+=n; }
	void Report(const Slice& name);

	void AddMessage(Slice msg) {
		AppendWithSpace(&message_, msg);
	}
};

void Stats::Start()
{
	start_=microSeconds();
	finish_=start_;
	next_report_=100;
	ops_=0;
	bytes_=0;
	last_op_finish_=start_;
	hist_.Clear();
}

void Stats::Merge(const Stats& other)
{
	hist_.Merge(other.hist_);
	ops_+=other.ops_;
	bytes_+=other.bytes_;
	if(other.start_ < start_) start_=other.start_;
	if(other.finish_ > finish_) finish_=other.finish_;
}

void Stats::FinishedSingleOp()
{
	if(FLAGS_histogram)
	{
		double now=microSeconds();
		double micros=now-last_op_finish_;
		hist_.Add(micros);
		if(micros > 20000)
		{
			fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
			fflush(stderr);
		}
		last_op_finish_=now;
	}
	
	ops_++;
	if(ops_>=next_report_)
	{	
		if (next_report_ < 1000)        next_report_ += 100;
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

void Stats::Report(const Slice& name)
{
	if(ops_<1)
		ops_=1;
	double elapsed=finish_-start_;//微秒
	//fprintf(stdout, "microSeconds=%f, ops=%d, bytes=%d\n", elapsed, ops_, bytes_);
	double speed1=elapsed/ops_;
	
	std::string extra;
	if(bytes_>0)
	{
		//double speed2=((bytes_*1000000)/(1024*1024))/(elapsed);
		char rate[100];
		snprintf(rate, sizeof rate, "%6.1f MB/s", (bytes_*1000000/1048576.0)/elapsed);
		extra=rate;
	}
	AppendWithSpace(&extra, message_);
	
	//fprintf(stdout, "%-12s  :  %11.3f micros/op;  %6.1f MB/s\n", name.ToString().c_str(), speed1, speed2);
	fprintf(stdout, "%-12s  :  %11.3f micros/op;%s%s\n", name.ToString().c_str(), speed1, (extra.empty() ? "" : " "), extra.c_str());
	
	if(FLAGS_histogram)
		fprintf(stdout, "Microseconds per op: \n%s\n", hist_.ToString().c_str());
	fflush(stdout);
} 




class SharedState{
public:
	pthread_mutex_t mu;
	pthread_cond_t cond;
	int total;
	int num_initialized;
	int num_done;
	bool start;
	
	SharedState(int t)
	{
		total=t;
		num_initialized=0;
		num_done=0;
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


struct ThreadState{
	//std::string name;
	int tid;
	Random rand; //不同线程有不同的Random
	Stats stats;
	SharedState* shared;
	
	ThreadState(int index)
		:tid(index),
		rand(1000+index),
		stats(Stats())
	{}
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
	
	int reads_;
	int heap_counter_;
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
		if (num_ != FLAGS_num) {
		  char msg[100];
		  snprintf(msg, sizeof(msg), "(%d ops)", num_);
		  thread->stats.AddMessage(msg);
		}
	
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

	
	void RunBenchmark(int nThreads, Slice name, void (Benchmark::*method)(ThreadState*))
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
		
		while(shared.num_initialized < nThreads)
			pthread_cond_wait(&shared.cond, &shared.mu);
		
		shared.start=true;
		pthread_cond_broadcast(&shared.cond);
				
		
		
		while(shared.num_done < nThreads)
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
		shared->num_initialized++;
		if(shared->num_initialized >= shared->total)
			pthread_cond_broadcast(&shared->cond);
		while(shared->start==false)
			pthread_cond_wait(&shared->cond, &shared->mu);
		pthread_mutex_unlock(&shared->mu);
			
		
		thread->stats.Start();//计时更准确
		(arg->bm->*(arg->method))(thread);
		thread->stats.Stop();
		
		
		pthread_mutex_lock(&shared->mu);
		shared->num_done++;
		if(shared->num_done>=shared->total)
			pthread_cond_broadcast(&shared->cond);
		pthread_mutex_unlock(&shared->mu);
	}
  
	void OpenBench(ThreadState* thread)
	{
		for(int i=0; i<num_; i++)
		{
			delete db_;
			Open();
			thread->stats.FinishedSingleOp();
		}
	}
	
	void ReadSequential(ThreadState* thread)
	{
		Iterator* iter=db_->NewIterator(ReadOptions());
		int i=0;
		int64_t bytes=0;
		for(iter->SeekToFirst(); i<reads_  &&  iter->Valid(); iter->Next())
		{
			bytes+=iter->key().size() + iter->value().size();
			thread->stats.FinishedSingleOp();
			++i;
		}
		delete iter;
		thread->stats.AddBytes(bytes);
	}
	
	void ReadReverse(ThreadState* thread)
	{
		Iterator* iter=db_->NewIterator(ReadOptions());
		int i=0;
		int64_t bytes=0;
		for(iter->SeekToFirst(); i<reads_  &&  iter->Valid(); iter->Prev())
		{
			bytes+=iter->key().size() + iter->value().size();
			thread->stats.FinishedSingleOp();
			++i;
		}
		delete iter;
		thread->stats.AddBytes(bytes);
	}
	
	void ReadRandom(ThreadState* thread)
	{
		ReadOptions options;
		std::string value;
		int found=0;
		for(int i=0; i<reads_; i++)
		{
			char key[100];
			const int k=thread->rand.Next() % FLAGS_num;
			snprintf(key, sizeof(key), "%016d", k);
			if(db_->Get(options, key, &value).ok())
				found++;
			thread->stats.FinishedSingleOp();
		}
		char msg[100];
		snprintf(msg, sizeof msg, "(%d of %d found)", found, num_);
		thread->stats.AddMessage(msg);
	}
	
	void ReadMissing(ThreadState* thread) 
	{
		ReadOptions options;
		std::string value;
		for (int i = 0; i < reads_; i++) {
		  char key[100];
		  const int k = thread->rand.Next() % FLAGS_num;
		  snprintf(key, sizeof(key), "%016d.", k);
		  db_->Get(options, key, &value);
		  thread->stats.FinishedSingleOp();
		}
	}
	
	void SeekRandom(ThreadState* thread)
	{
		ReadOptions options;
		int found=0;
		for(int i=0; i<reads_; i++)
		{
			Iterator* iter=db_->NewIterator(options);
			char key[100];
			const int k=thread->rand.Next() % FLAGS_num;
			snprintf(key, sizeof key, "%016d", k);
			iter->Seek(key);
			if(iter->Valid()  &&  iter->key()==key)
				found++;
			delete iter;
			thread->stats.FinishedSingleOp();
		}
		char msg[200];
		snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
		thread->stats.AddMessage(msg);
	}
	
	void ReadHot(ThreadState* thread)
	{
		ReadOptions options;
		std::string value;
		const int range=(FLAGS_num + 99)/100;
		for(int i=0; i<reads_; i++)
		{
			char key[100];
			const int k=thread->rand.Next() % range;
			snprintf(key, sizeof key, "%016d", k);
			db_->Get(options, key, &value);
			thread->stats.FinishedSingleOp();
		}
	}
	
	
	void DeleteSeq(ThreadState* thread) {
		DoDelete(thread, true);
	}

	void DeleteRandom(ThreadState* thread) {
		DoDelete(thread, false);
	}
	
	void DoDelete(ThreadState* thread, bool seq)
	{
		RandomGenerator gen;
		WriteBatch batch;
		Status s;
		for(int i=0; i<num_; i+=entries_per_batch_)
		{
			batch.Clear();
			for(int j=0; j<entries_per_batch_; j++)
			{
				const int k=seq ? i+j  :  (thread->rand.Next()  % FLAGS_num);
				char key[100];
				snprintf(key, sizeof key, "%016d", k);
				batch.Delete(key);
				thread->stats.FinishedSingleOp();
			}
			s=db_->Write(write_options_, &batch);
			if(!s.ok())
			{
				fprintf(stderr, "del error: %s\n", s.ToString().c_str());
				exit(1);
			}
		}
	}
	
	void ReadWhileWriting(ThreadState* thread)
	{
		if(thread->tid>0)
		{
			ReadRandom(thread);
		}
		else
		{
			RandomGenerator gen;
			while(true)
			{
				{
				pthread_mutex_lock(&thread->shared->mu);
				if(thread->shared->num_done +1 >= thread->shared->num_initialized)
					break;
				pthread_mutex_unlock(&thread->shared->mu);
				}
				
				const int k=thread->rand.Next() % FLAGS_num;
				char key[100];
				snprintf(key, sizeof key, "%016d", k);
				Status s=db_->Put(write_options_, key, gen.Generate(value_size_));
				if(!s.ok())
				{
					fprintf(stderr, "put error: %s\n", s.ToString().c_str());
					exit(1);
				}
			}
			
			thread->stats.Start();
		}
	}
	
	  void Compact(ThreadState* thread) {
		db_->CompactRange(NULL, NULL);
	  }
	
	  void Crc32c(ThreadState* thread) {
		// Checksum about 500MB of data total
		const int size = 4096;
		const char* label = "(4K per op)";
		std::string data(size, 'x');
		int64_t bytes = 0;
		uint32_t crc = 0;
		while (bytes < 500 * 1048576) {
		  crc = crc32c::Value(data.data(), size);
		  thread->stats.FinishedSingleOp();
		  bytes += size;
		}
		// Print so result is not dead
		fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

		thread->stats.AddBytes(bytes);
		thread->stats.AddMessage(label);
	  }
	  
	
  void AcquireLoad(ThreadState* thread) {
    int dummy;
    LearningLeveldb::AtomicPointer ap(&dummy);
    int count = 0;
    void *ptr = NULL;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
      for (int i = 0; i < 1000; i++) {
        ptr = ap.Acquire_Load();
      }
      count++;
      thread->stats.FinishedSingleOp();
    }
    if (ptr == NULL) exit(1); // Disable unused variable warning.
  }
  
  
  void SnappyCompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "(output: %.1f%%)",
               (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void SnappyUncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    std::string compressed;
    bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
    int64_t bytes = 0;
    char* uncompressed = new char[input.size()];
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok =  port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                    uncompressed);
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }
    delete[] uncompressed;

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

    static void WriteToFile(void* arg, const char* buf, int n) {
		reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
	  }
  
    void HeapProfile() {
		char fname[100];
		snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db, ++heap_counter_);
		WritableFile* file;
		Status s = g_env->NewWritableFile(fname, &file);
		if (!s.ok()) {
		  fprintf(stderr, "%s\n", s.ToString().c_str());
		  return;
		}
		bool ok = port::GetHeapProfile(WriteToFile, file);
		delete file;
		if (!ok) {
		  fprintf(stderr, "heap profiling not supported\n");
		  g_env->DeleteFile(fname);
		}
	  }
  
  void PrintStats(const char* key) {
    std::string stats;
    if (!db_->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());
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
	write_options_(WriteOptions()),
	reads_(FLAGS_reads<0 ? FLAGS_num : FLAGS_reads),
	heap_counter_(0)
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
		bool fresh_db=false;
		
		Slice name;
		while(benchmarks)
		{
			sep=strchr(benchmarks, ',');
			if(sep==NULL)
			{
				name=benchmarks;//Slice类拷贝赋值；构造临时变量，编译器生成的拷贝赋值函数
				benchmarks=NULL;
			}
			else
			{				
				//name=std::string(benchmarks, sep-benchmarks);	
				name=Slice(benchmarks, sep-benchmarks);//临时变量，赋值
				benchmarks=sep+1;
			}
			
			num_=FLAGS_num;
			entries_per_batch_=1;	
			value_size_=FLAGS_value_size;
			reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
			
			int num_threads=FLAGS_threads;
			void (Benchmark::*method)(ThreadState*)=NULL;
			
			if(name==Slice("open"))
			{
				method=&Benchmark::OpenBench;
				num_/=10000;
				if(num_<1) num_=1;
			}
			else if(name==Slice("fillseq"))
			{
				fresh_db=true;
				method=&Benchmark::WriteSeq;
			}
			else if (name == "fillbatch") {
				fresh_db=true;
				entries_per_batch_ = 1000;
				method = &Benchmark::WriteSeq;
			}
			else if(name==Slice("fillrandom"))
			{
				fresh_db=true;
				method=&Benchmark::WriteRandom;
			}
			else if (name == "fillsync")
			{
				fresh_db=true;
				num_ /= 1000;
				write_options_.sync = true;
				method = &Benchmark::WriteRandom;
			} else if (name == Slice("fill100K"))
			{
				fresh_db=true;
				num_ /= 1000;
				value_size_ = 100 * 1000;
				method = &Benchmark::WriteRandom;
			}
			else if (name == Slice("overwrite")) {
				fresh_db = false;
				method = &Benchmark::WriteRandom;
			}
			else if(name==Slice("readseq"))
				method=&Benchmark::ReadSequential;
			else if (name == Slice("readreverse")) {
				method = &Benchmark::ReadReverse;
		    }
			else if (name == Slice("readrandom")) 
				method = &Benchmark::ReadRandom;
			else if (name == Slice("readrandomsmall")) {
				reads_ /= 1000;
				method = &Benchmark::ReadRandom;
			}
			else if (name == Slice("readmissing")) 
				method = &Benchmark::ReadMissing;
			else if (name == Slice("seekrandom")) {
				method = &Benchmark::SeekRandom;
			}
			else if (name == Slice("readhot")) {
				method = &Benchmark::ReadHot;
		    }
			else if (name == Slice("deleteseq")) {
				method = &Benchmark::DeleteSeq;
		    }
			else if (name == Slice("deleterandom")) {
				method = &Benchmark::DeleteRandom;
		    }
			else if (name == Slice("readwhilewriting")) {
				num_threads++;  // Add extra thread for writing
				method = &Benchmark::ReadWhileWriting;
			}
			else if (name == Slice("compact")) {
				method = &Benchmark::Compact;
			}else if (name == Slice("crc32c")) {
				method = &Benchmark::Crc32c;
			}
			else if (name == Slice("acquireload")) {
				method = &Benchmark::AcquireLoad;
			}
			else if (name == Slice("snappycomp")) {
				method = &Benchmark::SnappyCompress;
		    } else if (name == Slice("snappyuncomp")) {
				method = &Benchmark::SnappyUncompress;
		    }
			else if (name == Slice("heapprofile")) {
				HeapProfile();
			}else if (name == Slice("stats")) {
				PrintStats("leveldb.stats");
			} else if (name == Slice("sstables")) {
				PrintStats("leveldb.sstables");
			}
			else if (name == Slice("num-files-at-level0")) {
				PrintStats("leveldb.sstables");
			}
			else if (name == Slice("approximate-memory-usage")) {
				PrintStats("leveldb.approximate-memory-usage");
			}
			else {
				if (name != Slice()) {  // No error message for empty name
				  fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
				}
			}
			
			if(fresh_db)
			{
				if(FLAGS_use_existing_db)
				{
					fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n", name.ToString().c_str());
					method = NULL;
				}
				else
				{
					delete db_;
					  db_ = NULL;
					  DestroyDB(FLAGS_db, Options());
					  Open();
				}
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
		else if (sscanf(argv[i], "--reads=%d%c", &n, &c) == 1) {
		  FLAGS_reads = n;
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