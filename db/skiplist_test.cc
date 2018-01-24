
#include "skiplist.h"
#include <set>
#include "leveldb/env.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testharness.h"

namespace leveldb {

typedef uint64_t Key;

struct Comparator{
	int operator()(const Key& a, const Key& b) const
	{
		if (a < b) {
		  return -1;
		} else if (a > b) {
		  return +1;
		} else {
		  return 0;
		}
	}
};

class SkipTest { };



/*
保证 一个writer和多个reader，除了有迭代器的时候，没有同步 时，当iterator是constructor时，
reader可以观察到所有数据。
由于插入式并发进行的，可以观察到自iterator创建之后插入的数据。
不能错过iterator创建时的值。 
*/
// We generate multi-part keys:
//     <key,gen,hash>
// where:
//     key is in range [0..K-1]
//     gen is a generation number for key
//     hash is hash(key,gen)
class ConcurrentTest{
private:
	static const uint32_t K=4;
	
	static uint64_t key(Key key) { return (key >> 40); }
	static uint64_t gen(Key key) { return (key >> 8) & 0xffffffffu; }
	static uint64_t hash(Key key) { return key & 0xff; }

	
  static Key MakeKey(uint64_t k, uint64_t g) {
    assert(sizeof(Key) == sizeof(uint64_t));
    assert(k <= K);  // We sometimes pass K to seek to the end of the skiplist
    assert(g <= 0xffffffffu);
    return ((k << 40) | (g << 8) | (HashNumbers(k, g) & 0xff));
  }
  static Key RandomTarget(Random* rnd) {
    switch (rnd->Next() % 10) {
      case 0:
        // Seek to beginning
        return MakeKey(0, 0);
      case 1:
        // Seek to end
        return MakeKey(K, 0);
      default:
        // Seek to middle
        return MakeKey(rnd->Next() % K, 0);
    }
  }
  
  
	struct State{
		port::AtomicPointer generation[K];
		void Set(int k, intptr_t v)
		{
			generation[k].Release_Store(reinterpret_cast<void*>(v));
		}
		
		
		State()
		{
			for(int k=0; k<K; k++)
				Set(k, 0);
		}
	};
	
	State current_;
	
	Arena arena_;
	
	SkipList<Key, Comparator> list_;
	
public:
	ConcurrentTest() : list_(Comparator(), &arena_){}
	
	void WriteStep(Random *rnd)
	{
		const uint32_t k=rnd->Next() % K;
		const intptr_t g=current_.Get(k) + 1;
		const Key key=MakeKey(k, g);
		list_.Insert(key);
		current_.Set(k, g);
	}
	
	void ReadStep(Random *rnd)
	{
		State initial_state;
		for(int k=0; k<K; k++)
			initial_state.Set(k, current_.Get(k));
		
		Key pos=RandomTarget(rnd);
		SkipList<Key, Comparator>::Iterator iter(&list_);
		iter.Seek(pos);
		while(true)
		{
			Key current;
			if(!iter.Valid())
				current=MakeKey(K, 0);
			else
			{
				current=iter.key();
				ASSERT_TRUE(IsValidKey(current)) << current;
			}
			ASSERT_LE(pos, current) << "should not go backwards";
			
			while(pos < current)
			{
				
			}
		}
	}
};


class TestState{
public:
	ConcurrentTest t_;
	int seed_;
	port::AtomicPointer quit_flag_;
	
	enum ReaderState{
		STARTING,
		RUNNING,
		DONE
	};
	
	explicit TestState(int s)
		:seed_(s),
		quit_flag_(NULL),
		state_(STARTING),
		state_cv_(&mu_){}
	
	void Wait(ReaderState s)
	{
		mu_.Lock();
		while(state_ != s)
			state_cv_.Wait();
		mu_.Unlock();
	}
	
	void Change(ReaderState s)
	{
		mu_.lock();
		state_=s;
		state_cv_.Singal();
		mu_.Unlock();
	}


private:
	port::Mutex mu_;
	ReaderState state_;
	port::CondVar state_cv_;
};

static void ConcurrentReader(void *arg)
{
	TestState* state=reinterpret_cast<TestState*>(arg);
	Random rnd(state->seed_);
	int64_t reads=0;
	state->Change(TestState::RUNNING);
	while(!state->quit_flag_.Acquire_Load()) // 等待 quit_flag_不为空退出循环
	{
		state->t_.ReadStep(&rnd); // read
		++reads;
	}
	state->Change(TestState::DONE);
}

static void RunConcurrent(int run)
{
	const int seed=test::RandomSeed() + (run*100);
	Random rnd(seed);
	const int N=1000;
	const int kSize=1000;
	for(int i=0; i<N; i++)
	{
		if((i % 100)==0)
			fprintf(stderr, "Run %d of %d\n", i, N);
		TestState state(seed+1);
		Env::Default()->Schedule(ConcurrentReader, &state); // 调度 read
		state::Wait(TestState::RUNNING);
		for(int i=0; i<kSize; i++)
			state.t_.WriteStep(&rnd); // write
		state.quit_flag_.Release_Store(&state); // 更改 quit_flag_不为空
		state.Wait(TestState::DONE);
	}
}


TEST(SkipTest, Concurrent1) { RunConcurrent(1); }
TEST(SkipTest, Concurrent2) { RunConcurrent(2); }
TEST(SkipTest, Concurrent3) { RunConcurrent(3); }
TEST(SkipTest, Concurrent4) { RunConcurrent(4); }
TEST(SkipTest, Concurrent5) { RunConcurrent(5); }

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}