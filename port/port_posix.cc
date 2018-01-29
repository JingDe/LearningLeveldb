
#include "port/port_posix.h"

namespace leveldb{
namespace port{
	
static void PthreadCall(const char* label, int result)
{
	if(result!=0)
	{
		 fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
		abort();
	}
}

Mutex::Mutex() 
{ PthreadCall("init mutex", pthread_mutex_init(&mu_, NULL)); }

Mutex::~Mutex()
{ PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

Mutex::Lock()
{ PthreadCall("lock", pthread_mutex_lock(&mu_)); }

Mutex::Unlock()
{ PthreadCall("unlock", pthread_mutex_unlock(&mu_)); }


CondVar::CondVar(Mutex* mu)
:mu_(mu)
{
	PthreadCall("init cv", pthread_cond_init(&cv_, NULL));
}

CondVar::~CondVar()
{
	PthreadCall("destroy cv", pthread_cond_destroy(&cv_));
}

void CondVar::Wait()
{
	PthreadCall("wait cv", pthread_cond_wait(&cv_, &mu_->mu_));
}

void CondVar::Signal()
{
	PthreadCall("wait cv", pthread_cond_signal(&cv_));
}

void CondVar::SignalAll()
{
	PthreadCall("wait cv", pthread_cond_broadcast(&cv_));
}

// typedef pthread_once_t OnceType;
void InitOnce(OnceType* once, void (*initializer)())
{
	PthreadCall("once", pthread_once(once, initializer));
}


}
}