
class PosixEnv: public Env{
public:
	PosixEnv();
	virtual ~PosixEnv()
	{
		
	}
	
	virtual void Schedule(void (*function)(void*), void* arg);

	virtual void StartThread(void (*function)(void* arg), void* arg);
	
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