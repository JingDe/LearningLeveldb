

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
	LRUHandle* next_hash;
	LRUHandle* next;
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


// HandleTable包含一个bucket数组，每个bucket是一个cache entry(LRUHandle)的链表，
// 这个链表的entry哈希到这个桶
class HandleTable{
	
	LRUHandle* Lookup(const Slice& key, uint32_t hash)
	{
		return *FindPointer(key, hash);
	}

private:
	
	// 返回指向一个slot的指针，这个slot指向一个匹配key/hash的cache entry。否则返回指向最后slot的指针
	LRUHandle** FindPointer(const Slice&  key, uint32_t hash)
	{
		LRUHandle** ptr=&list_[hash  &  (length_-1)]; // 
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
	
};


// sharded cache的一个shard
class LRUCache{
public:
	
	
	void SetCapacity(size_t capacity) {capacity_=capacity; }
	Cache::Handle* Lookup(const Slice& key, uint32_t hash);

private:
	size_t capacity_;
	
	size_t usage_;
	
	LRUHandle lru_;
	
	LRUHandle in_use_;
	
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


Cache* NewLRUCache(size_t capacity)
{
	return new ShardedLRUCache(capacity);
}

}