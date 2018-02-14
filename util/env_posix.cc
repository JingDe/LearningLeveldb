#include "leveldb/env.h"

namespace leveldb{

namespace{

class Limiter{
public:
	Limiter(intptr_t n)
	{
		SetAllowed(n);
	}
	
	// 如果可以获得另一个资源，获取并返回true，否则返回false
	bool Acquire()
	{
		if(GetAllowed() <= 0)
		{
			return false;
		}
		MutexLock l(&mu_);
		intptr_t x=GetAllowed();
		if(x<=0)
			return false;
		else
		{
			SetAllowed(x-1);
			return true;
		}
	}
	
	// 释放一个通过Acquire()获得的资源
	void Release()
	{
		MutexLock l(mu_);
		SetAllowed(GetAllowed() +1);
	}
	
private:
	port::Mutex mu_;
	port::AtomicPointer allowed_;

	intptr_t GetAllowed() const
	{
		return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
	}
	
	void SetAllowed(intptr_t v)
	{
		allowed_.Release_Store(reinterpret_cast<void*>(v));
	}
};

static Status PosixError(const std::string& context, int er_number)
{
	if(err_number==ENOENT)
		return Status::NotFound(context, strerror(err_number));
	else
		return Status::IOError(context, strerror(err_number));
}

class PosixWritableFile : public WritableFile{
private:
	std::string filename_;
	int fd_;
	char buf_[kBufSize];
	size_t pos_;

public:
	PosixWritableFile(const std::string& fname, int fd)
		:filename_(fname), fd_(fd), pos_(0) {}
	~PosixWritableFile()
	{
		if(fd_ >=0)
			Close();
	}
	
	virtual Status Append(const Slice& data)
	{
		size_t n=data.size();
		const char* p=data.data();
		
		size_t copy=std::min(n, kBufSize-pos_);
		memcpy(buf_ + pos_, p, copy);
		p += copy;
		n -= copy;
		pos_ += copy;
		if(n==0)
			return Status::OK();
		
		Status s=FlushBuffered();
		if(!s.ok())
			return s;
		
		if(n<kBufSize) // 剩余字节少，延迟写文件，先写到buf_
		{
			memcpy(buf_, p, n);
			pos_=n;
			return Status::OK();
		}
		return WriteRaw(p, n); // 直接写文件
	}

	virtual Status Flush(){
		return FlushBuffered();
	}
	
	Status SyncDirIfManifest()
	{
		const char* f=filename_.c_str();
		const char* sep=strrchr(f, '/');
		Slice basename;
		std::string dir;
		if(sep == NULL)
		{
			dir=".";
			basename=f;
		}
		else
		{
			dir=std::string(f, sep-f);
			basename = sep +1;
		}
		Status s;
		if(basename.starts_with("MANIFEST"))
		{
			int fd=open(dir.c_str(), O_RDONLLY);
			if(fd < 0)
				s=PosixError(dir, errno);
			else
			{
				if(fsync(fd)<0) // 刷新文件所在目录
					s=PosixError(dir, errno);
				close(fd);
			}
		}
		return s;
	}
	
	virtual Status Close()
	{
		Status result=FlushBuffered();
		const int r=close(fd_);
		if(r<0  &&  result.ok())
			result=PosixError(filename_, errno);
		fd=-1;
		return result;
	}
	
	virtual Status Sync()
	{
		// 如果是manifest文件，确保manifest表示的新文件存在filesystem中
		Status s=SyncDirIfManifest();
		if(!s.ok())
			return s;
		s=FlushBuffered();
		if(s.ok())
		{
			if(fdatasync(fd_) !=0)
				s=PosixError(filename_, errno);
		}
		return s;
	}
	
private:
	Status FlushBuffered()
	{
		Status s=WriteRaw(buf_, pos_);
		pos_=0;
		return s;
	}
	
	Status WriteRaw(const char* p, size_t n)
	{
		while(n >0)
		{
			ssize_t r=write(fd_, p, n);
			if(r <0)
			{
				if(errno == EINTR)
					continue;
				return PosixError(filename_, errno);
			}
			p += r;
			n -= r;
		}
		return Status::OK();
	}
};


class PosixSequentialFile: public SequentialFile {
private:
	std::string filename_;
	int fd_;
	
public:
	virtual Status Skip(uint64_t n)
	{
		if(lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1))
			return PosixError(filename_, errno);
		return Status::OK();
	}
};

// 通过 pread() 随机访问
class PosixRandomAccessFile: public RandomAccessFile 
{
private:
	std::string filename_;
	bool temporary_fd_;  // true表示fd_是-1，每次read时open
	int fd_;
	Limiter* limiter_;

public:
	PosixRandomAccessFile(const std::string& fname, int fd, Limiter* limiter)
		:filename_(fname), fd_(fd), limiter_(limiter)
	{
		tempory_fd_ = ! limiter->Acquire();
		if(tempory_fd_)
		{
			close(fd_);
			fd_=-1;
		}
	}
	
	virtual ~PosixRandomAccessFile()
	{
		if(!temporary_fd_)
		{
			close(fd_);
			limiter_->Release();
		}
	}
	
	virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
	{
		int fd=fd_;
		if(temporary_fd_)
		{
			fd=open(filename_.c_str(), O_RDONLY);
			if(fd<0)
				return PosixError(filename_, errno);
		}
		Status s;
		ssize_t r=pread(fd, scratch, n, static_cast<off_t>(offset));
		*result = Slice(scratch, (r<0) ? 0 : r);
		if(r<0)
			s=PosixError(filename_, errno);
		if(temporary_fd_)
			close(fd);
		return s;
	}
};

// 通过 mmap() 随机访问
class PosixMmapReadableFile : public RandomAccessFile
{
private:
	std::string filename_;
	void* mmapped_region_;
	size_t length_;
	Limiter* limiter_;
  
public:
	PosixMmapReadableFile(const std::string& fname, void * base, size_t length, Limiter* limiter)
		: filename_(fname), mmaped_region_(base), length_(length), limiter_(limiter)
	{}
	
	virtual ~PosixMmapReadableFile()
	{
		munmap(mmapped_region_, length_);
		limiter_->Release();
	}
	
	virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
	{
		Status s;
		if(offset + n > length_)
		{
			*result=Slice();
			s=PosixError(filename_, EINVAL);
		}
		else
			*result=Slice(reinterpret_cast<char*>(mmaped_region_) + offset, n);
		return s;
	}
};

class PosixLockTable{
private:
	port::Mutex mu_;
	std::set<std::string> locked_files_;
public:
	bool Insert(const std::string& fname)
	{
		MutexLock l(&mu_);
		return locked_files_.insert(fname).second;
	}
};

class PosixEnv: public Env{
public:
	PosixEnv();
	virtual ~PosixEnv()
	{
		
	}
	
	virtual Status CreateDir(const std::string& name)
	{
		Status result;
		if(mkdir(name.c_str(), 0755) !=0)
			result=PosixError(name, errno);
		return result;
	}
	
	virtual Status NewWritableFile(const std::string& fname, WritableFile** result)
	{
		Status s;
		int fd=open(fname.c_str(), 0_TRUNC | O_WRONLY | O_CREAT, 0644);
		if(fd<0)
		{
			*result=NULL;
			s=PosixError(fname, errno);
		}
		else
		{
			*result=new PosixWritableFile(fname, fd);
		}
		return s;
	}
	
	virtual Status NewSequentialFile(const std::string& fname,
							   SequentialFile** result) {
		int fd = open(fname.c_str(), O_RDONLY);
		if (fd < 0) {
			*result = NULL;
			return PosixError(fname, errno);
		} else {
			*result = new PosixSequentialFile(fname, fd);
			return Status::OK();
		}
	}
	
	virtual Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result)
	{
		*result=NULL;
		Status s;
		int fd=open(fname.c_str(), O_RDONLY);
		if(fd<0)
			s=PosixError(fname, errno);
		else if(mmap_limit_.Acquire())
		{
			uint64_t size;
			s=GetFileSize(fname, &size);
			if(s.ok())
			{
				void* base=mmap(NULL, size, PORT_READ, MAP_SHARED, fd, 0); // 把文件fd映射到进程base地址处
				if(base != MAP_FAILED)
					*result=new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
				else
					s=PosixError(fname, errno);
			}
			close(fd);
			if(!s.ok())
				mmap_limit_.Release();
		}
		else
			*result=new PosixRandomAccessFile(fname, fd, &fd_limit_);
		return s;
	}
	
	virtual Status GetFileSize(const std::string& fname, uint64_t* size)
	{
		Status s;
		struct stat sbuf;
		if(stat(fname.c_str(), &sbuf) != 0)
		{
			*size=0;
			s=PosixError(fname, errno);
		}
		else
			*size=sbuf.st_size;
		return s;
	}
	
	virtual Status DeleteFile(const std::string& fname)
	{
		Status result;
		if(unlink(fname.c_str()) !=0)
			result=PosixError(fname, errno);
		return result;
	}
	
	
	virtual bool FileExists(const std::string& fname)
	{
		return access(fname.c_str(), F_OK);
	}
	
	virtual Status RenameFile(const std::string& src, const std::string& target)
	{
		Status result;
		if(rename(src.c_str(), target.c_str()) !=0)
			result=PosixError(src, errno);
		return result;
	}
	
	virtual Status LockFile(const std::string& fname, FileLock** lock)
	{
		*lock=NULL;
		Status result;
		int fd=open(fname.c_str(), O_RDWR  |  O_CREAT, 0644);
		if(fd<0)
		{
			result=PosixError(fname, errno);
		}
		else if(!locks_.Insert(fname))
		{
			close(fd);
			result=Status::IOError("lock"+fname, "already held by process");
		}
		else if(LockOrUnlock(fd, true)==-1)
		{
			result=PosixError("lock "+fname, errno);
			close(fd);
			locks_.Remove(fname);
		}
		else
		{
			PosixFileLock* my_lock=new PosixFileLock;
			my_lock->fd_=fd;
			my_lock->name_=fname;
			*lock=my_lock;
		}		
	}
	
	virtual void Schedule(void (*function)(void*), void* arg);

	virtual void StartThread(void (*function)(void* arg), void* arg);
	
	/*pid_t gettid()
	{
	  return static_cast<pid_t>(syscall(SYS_gettid));
	}*/
	
	static uint64_t gettid()
	{
		pthread_t tid=pthread_self();
		uint64_t thread_id=0;
		memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
		return thread_id;
	}
	
	virtual Status NewLogger(const std::string& fname, Logger** result)
	{
		FILE* f=fopen(fname.c_str(), "w");
		if(f==NULL)
		{
			*result=NULL;
			return PosixError(fname, errno);
		}
		else
		{
			*result=new PosixLogger(f, &PosixEnv::gettid);
			return Status::OK();
		}
	}
	
private:
	void PthreadCall(const char* label, int result)
	{
		if(result!=0)
		{
			 fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
			abort();
		}
	}
	
	void BGThread();
	static void* BGThreadWrapper(void* arg) {
		reinterpret_cast<PosixEnv*>(arg)->BGThread();
		return NULL;
	}
	
	pthread_mutex_t mu_;
	pthread_cond_t bgsignal_;
	pthread_t bgthread_;
	bool started_bgthread_;
	
	  // Entry per Schedule() call
	struct BGItem { void* arg; void (*function)(void*); };
	typedef std::deque<BGItem> BGQueue;
	BGQueue queue_;
	
	PosixLockTable locks_;
	Limiter mmap_limit_;
};


void PosixEnv::Schedule(void (*function)(void*), void* arg)
{
	PthreadCall("lock", pthread_mutex_lock(&mu_));
	
	if(!started_bgthread_)
	{
		started_bgthread_=true;
		PthreadCall("create thread", pthread_create(&bgthread_, NULL, &PosixEnv::BGThreadWrapper, this));
	}
	
	if(queue_.empty())
		PthreadCall("signal", pthread_cond_signal(&bgsignal_));
	
	queue_.push_back(BGItem());
	queue_.back().function = function;
	queue_.back().arg = arg;

	PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}



namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}

static void* StartThreadWrapper(void* arg)
{
	StartThread* state=reinterpret_cast<StartThreadState*>(arg);
	state->user_function(state->arg);
	delete state;
	return NULL;
}


void PosixEnv::StartThread(void (*function)(), void * arg)
{
	pthread_t t;
	StartThreadState* state=new StartThreadState;
	state->user_function=function;
	state->arg=arg;
	PthreadCall("start thread", pthread_create(&t, NULL, &StartThreadWrapper, state));
}



static int LockOrUnlock(int fd, bool ok)
{
	errno=0;
	struct flock f;
	memset(&f, 0, sizeof(f));
	f.l_type = (lock ? F_WRLCK  :  F_UNLCK);
	f.l_whence=SEEK_SET;
	f.l_start=0;
	f.l_len=0;
	return fcntl(fd, F_SETLK, &f);
}

class Logger{
public:
	Logger() {}
	virtual ~Logger();

private:
	Logger(const Logger&);
	void operator=(const Logger&);
};


class PosixFileLock: public FileLock{
public:
	int fd_;
	std::string name_;
};

} // namespace


static pthread_once_t once=PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv()
{
	default_env=new PosixEnv;
}

Env* Env::Default()
{
	pthread_once(&once, InitDefaultEnv);
	return default_env;
}



}