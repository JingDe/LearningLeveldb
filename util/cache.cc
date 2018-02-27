
#include "util/hash.h" // Hash()函数

namespace leveldb
{

// LRU cache的实现
// 每个entry(LRUHandle)的in_cache表示cache是否有该entry的引用，成为false的可能有：传递给deleter
// 或者Erase()，或者Insert()时已有一个相同的key插入了，或者cache在释放

// cache有两条item的链表，cache的所有item在其中一条链表。用户引用但cache删除的item不在任何链表中
// in-use链表：被用户引用的item，无序。
//			用作invariant检查，如果删除这个检查，这条链表中的元素可能进入没有连接的单item链表
// LRU链表：不被用户引用的item，LRU序。

// 元素通过Ref()和Unref()在两条链表中移动，当检测到cache中一个元素获得或失去唯一的外部引用




// 一个entry是一个定长的堆上的结构。所有entry按照访问时间保存在一个循环双向链表中
struct LRUHandle{
	void* value;
	void (*deleter)(const Slice&, void *value);
	LRUHandle* next_hash; //链接HandleTable的所有LRUHandle的指针
	LRUHandle* next; // next和prev链接LRUCache的两条链表的指针
	LRUHandle* prev;
	size_t key_length;
	bool in_cache; // 该entry是否在cache中
	
	uint32_t hash; // key()的哈希；用来快速分片和比较
	char key_data[1]; // key的开头
	
	Slice key() const
	{
		assert(next != this); // next_等于this时，LRUHandle是一个空链表的头结点，头结点没有有意义的 key
		return Slice(key_data, key_length);
	}
};


// HandleTable包含一个bucket数组，每个bucket是一个cache entry(LRUHandle)的链表，通过next_hash指针链接
// 这个链表的entry哈希到这个桶
class HandleTable{
	
	LRUHandle* Lookup(const Slice& key, uint32_t hash)
	{
		return *FindPointer(key, hash);
	}
	
	LRUHandle* Insert(LRUHandle* h)
	{
		LRUHandle** ptr=FindPointer(h->key(), h->hash);
		LRUHandle* old=*ptr;
		h->next_hash = (old==NULL  ?  NULL  :  old->next_hash); // 用h替换old位置
		*ptr=h; 
		if(old==NULL)
		{
			++elems_;
			if(elems_ > length_)
				Resize();
		}
		return old;
	}
	
	LRUHandle* Remove(const Slice& key, uint32_t hash)
	{
		LRUHandle** ptr=FindPointer(key, hash);
		LRUHandle* result=*ptr;
		if(result != NULL)
		{
			*ptr=result->next_hash;
			--elems_;
		}
		return result;
	}

private:
	
	// 返回指向一个slot的指针，这个slot指向一个匹配key/hash的cache entry。否则返回指向最后slot的指针
	LRUHandle** FindPointer(const Slice&  key, uint32_t hash)
	{
		LRUHandle** ptr=&list_[hash  &  (length_-1)]; // ??
		while(*ptr!=NULL  &&  ((*ptr)->hash != hash  ||  key!=(*ptr)->key() ))
			ptr=&(*ptr)->next_hash;
		return ptr;
	}
	
	uint32_t length_;
	uint32_t elems_;
	LRUHandle** list_;
};



static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits; // shard的个数



class ShardedLRUCache : public Cache{
private:
	LRUCache shard_[kNumShards]; // shard的个数
	uint64_t last_id_;
	
	static inline uint32_t HashSlice(const Slice& s)
	{
		return Hash(s.data(), s.size(), 0);
	}
	
	static uint32_t Shard(uint32_t hash) { // 保留hash的高kNumShardBits位，即哪一个shard
		return hash >> (32 - kNumShardBits);
	}

public:
	explicit ShardedLRUCache(size_t capacity)
		:last_id(0)
	{
		const size_t per_shard=(capacity + (kNumShards-1)) / kNumShards;
		for(int s=0; s<kNumShards; s++)
			shard_[s].SetCapacity(per_shard);
	}
	
	virtual Handle* Lookup(const Slice& key)
	{
		const uint32_t hash=HashSlice(key);
		return shard_[Shard(hash)].Lookup(key, hash);
	}
	
	virtual Handle* Insert(const Slice& key, void* value, size_t charge, 
							void (*deleter)(const Slice& key, void* value))
	{
		const uint32_t hash=HashSlice(key);
		return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
	}
	
	virtual void* Value(Handle* handle)
	{
		return reinterpret_cast<LRUHandle*>(handle)->value;
	}
	
	virtual uint64_t NewId() {
		MutexLock l(&id_mutex_);
		return ++(last_id_);
	}
};


// sharded cache的一个shard
class LRUCache{
public:
	Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
	
	void SetCapacity(size_t capacity) {capacity_=capacity; }
	Cache::Handle* Lookup(const Slice& key, uint32_t hash);

private:
	size_t capacity_; // 缓存的总容量
	
	size_t usage_; // 已使用容量
	
	LRUHandle lru_; // 通过next、prev链接
					// lru.prev是最新的entry，lru.next是最旧的entry
					// refs==1并且in_cache==true
	
	LRUHandle in_use_; // 通过next、prev链接
						// 被用户使用，refs大于等于2并且in_cache==true
	
	HandleTable table_;
};

LRUCache::LRUCache()
	:usage_(0)
{
	lru_.next=&lru_;
	lru_.prev=&lru_;
	in_use_.next=&in_use_;
	in_use_.prev=&in_use_;
}

// 查找key对应的LRUHandle
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash)
{
	MutexLock l(&mutex_);
	LRUHandle* e=table_.Lookup(key, hash);
	if(e!=NULL)
		Ref(e):
	return reinterpret_cast<Cache::Handle*>(e);
}

// 将e插在list之前，作为最新的entry
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e)
{
	e->next=list;
	e->prev=list->prev;
	e->prev->next=e;
	e->next->prev=e;
}

// 从next/prev链接的链表中删除e
void LRUCache::LRU_Remove(LRUHandle* e)
{
	e->next->prev=e->prev;
	e->prev->next=e->next;
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value))
{
	MutexLock l(&mutex_);
	
	LRUHandle* e=reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle)-1 + key.size() ));
	e->value=value;
	e->deleter=deleter;
	e->charge=charge;
	e->key_length=key.size();
	e->hash=hash;
	e->in_cache=false;
	e->refs=1; // 返回的handle，一个引用
	memcpy(e->key_data, key.data(), key.size());
	
	if(capacity_ >0)
	{
		e->refs++; // cache的第二个引用
		e->in_cache=true;
		LRU_Append(&in_use_, e); // 将e插入到in_use_中，最新的位置
		usage_ += charge;
		FinishErase(table_.Insert(e)); // 将e插入到table_中，替换table_中的旧handle，
							// FinishErase从in_use_中删除key的旧LRUHandle，释放或者插入到lru_中	
	}
	else
		e->next=NULL; // 不缓存。capacity_等于0表示关闭缓存
	
	// 缓存容量达到总容量，从table_和this中删除
	while(usage_ > capacity_  &&  lru_.next!=&lru_)
	{
		LRUHandle* old=lru_.next; // 从最旧的LRUHandle开始删除
		assert(old->refs == 1);
		bool erased=FinishErase(table_.Remove(old->key(), old->hash));
		if (!erased) {  // erased==false
			assert(erased); // ??
		}
	}
	
	return reinterpret_cast<Cache::Handle*>(e);
}

// 从缓存中删除
// 从所在链表（lru_）中删除，减少引用计数，如到0释放
bool LRUCache::FinishErase(LRUHandle* e)
{
	if(e != NULL)
	{
		assert(e->in_cache);
		LRU_Remove(e);
		e->in_cache =false;
		usage_ -= e->charge;
		Unref(e);
	}
}

void LRUCache::Release(Cache::Handle* handle) {
	MutexLock l(&mutex_);
	Unref(reinterpret_cast<LRUHandle*>(handle));
}

// 当引用计数减少到0时释放，否则从in_use_链表移到lru_链表中
void LRUHandle::Unref(LRUHandle* e)
{
	assert(e->refs >0);
	e->refs--;
	if(e->refs ==0)
	{
		assert(!e->in_cache);
		(*e->deleter)(e->key(), e->value);
		free(e);
	}
	else if(e->in_cache  &&  e->refs==1)
	{
		LRU_Remove(e);
		LRU_Append(&lru_, e);
	}
}

Cache* NewLRUCache(size_t capacity)
{
	return new ShardedLRUCache(capacity);
}

}