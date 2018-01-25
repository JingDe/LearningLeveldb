
#include "skiplist.h"
#include <set>
#include "leveldb/env.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "testharness.h"

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

	  static uint64_t HashNumbers(uint64_t k, uint64_t g) {
		uint64_t data[2] = { k, g };
		return Hash(reinterpret_cast<char*>(data), sizeof(data), 0);
	  }
	
  static Key MakeKey(uint64_t k, uint64_t g) {
    assert(sizeof(Key) == sizeof(uint64_t));
    assert(k <= K);  // We sometimes pass K to seek to the end of the skiplist
    assert(g <= 0xffffffffu);
    return ((k << 40) | (g << 8) | (HashNumbers(k, g) & 0xff));
  }
  
  static bool IsValidKey(Key k) {
    return hash(k) == (HashNumbers(key(k), gen(k)) & 0xff);
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
		intptr_t Get(int k) {
		  return reinterpret_cast<intptr_t>(generation[k].Acquire_Load());
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
	
	/*
	插入操作，选择一个随机key，设置每个key的gen等于最后的generation number+1，
	设置hash等于Hash(key, gen)
	*/
  void WriteStep(Random* rnd) {
    const uint32_t k = rnd->Next() % K;
    const intptr_t g = current_.Get(k) + 1;
    const Key key = MakeKey(k, g);
    list_.Insert(key);
    current_.Set(k, g);
  }
	
	/*
	read之前，对每一个key的最后一次插入的generation number做一次快照。
	然后迭代，随机调用Next()和Seek()。
	对每次遇到的key，检查是否是期望的初始快照、或者是迭代器开始后并发添加的。
	*/
	/*void ReadStep(Random *rnd)
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
				ASSERT_LT(key(pos), K) << pos;
				
				ASSERT_TRUE((gen(pos)==0)  ||  (gen(pos) > static_cast<Key>(initial_state.Get(key(pos)))) )
					<< "key: " << key(pos)
					<< "; gen:" << gen(pos)
					<< "; initgen: "
					<< initial_state.Get(key(pos));
				
				if(key(pos) < key(current))
					pos=MakeKey(key(pos)+1, 0);
				else
					pos=MakeKey(key(pos), gen(pos)+1);
			}
			
			if(!iter.Valid())
				break;
			
			if(rnd->Next() % 2)
			{
				iter.Next();
				pos=MakeKey(key(pos), gen(pos)+1);
			}
			else
			{
				Key new_target=RandomTarget(rnd);
				if(new_target > pos)
				{
					pos=new_target;
					iter.Seek(new_target);
				}
			}
		}
	}*/
  void ReadStep(Random* rnd) {
    // Remember the initial committed state of the skiplist.
    State initial_state;
    for (int k = 0; k < K; k++) {
      initial_state.Set(k, current_.Get(k));
    }

    Key pos = RandomTarget(rnd);
    SkipList<Key, Comparator>::Iterator iter(&list_);
    iter.Seek(pos);
    while (true) {
      Key current;
      if (!iter.Valid()) {
        current = MakeKey(K, 0);
      } else {
        current = iter.key();
        //ASSERT_TRUE(IsValidKey(current)) << current;
      }
      //ASSERT_LE(pos, current) << "should not go backwards";

      // Verify that everything in [pos,current) was not present in
      // initial_state.
      while (pos < current) {
        //ASSERT_LT(key(pos), K) << pos;

        // Note that generation 0 is never inserted, so it is ok if
        // <*,0,*> is missing.
        /*ASSERT_TRUE((gen(pos) == 0) ||
                    (gen(pos) > static_cast<Key>(initial_state.Get(key(pos))))
                    ) << "key: " << key(pos)
                      << "; gen: " << gen(pos)
                      << "; initgen: "
                      << initial_state.Get(key(pos));*/

        // Advance to next key in the valid key space
        if (key(pos) < key(current)) {
          pos = MakeKey(key(pos) + 1, 0);
        } else {
          pos = MakeKey(key(pos), gen(pos) + 1);
        }
      }

      if (!iter.Valid()) {
        break;
      }

      if (rnd->Next() % 2) {
        iter.Next();
        pos = MakeKey(key(pos), gen(pos) + 1);
      } else {
        Key new_target = RandomTarget(rnd);
        if (new_target > pos) {
          pos = new_target;
          iter.Seek(new_target);
        }
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
		mu_.Lock();
		state_=s;
		state_cv_.Signal();
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
		state.Wait(TestState::RUNNING);
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


void classSizeTest()
{
	std::cout<<"sizeof(AtomicPointer)="<<sizeof(port::AtomicPointer)<<std::endl;
	std::cout<<"sizeof(Node)="<<sizeof(SkipList<Key, Comparator>::Node)<<std::endl;
	std::cout<<"sizeof(Key)="<<sizeof(Key)<<std::endl;
	
	Arena arena;
  Comparator cmp;
  SkipList<Key, Comparator> list(cmp, &arena);
	
	Key k1=100;
	SkipList<Key, Comparator>::Node* newNode=list.NewNode(k1, 1);
	std::cout<<"sizeof(NewNode)="<<sizeof(*newNode)<<std::endl;
	Key k2=200;
	newNode=list.NewNode(k2, 2);
	std::cout<<"sizeof(NewNode)="<<sizeof(*newNode)<<std::endl;
}


}  // namespace leveldb

int main(int argc, char** argv) {
	leveldb::classSizeTest();
  return leveldb::test::RunAllTests();
}