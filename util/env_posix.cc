#include "leveldb/env.h"

namespace leveldb{

namespace{
	
static Status PosixError(const std::string& context, int er_number)
{
	if(err_number==ENOENT)
		return Status::NotFound(context, strerror(err_number));
	else
		return Status::IOError(context, strerror(err_number));
}

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