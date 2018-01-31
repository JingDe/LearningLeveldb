

namespace leveldb
{
	
static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache{
private:
	LRUCache shard_[kNumShards];
	uint64_t last_id_;
	

public:
	explicit ShardedLRUCache(size_t capacity)
		:last_id(0)
	{
		const size_t per_shard=(capacity + (kNumShards-1)) / kNumShards;
		for(int s=0; s<kNumShards; s++)
			shard_[s].SetCapacity(per_shard);
	}

};


struct LRUHandle{
	void* value;
	void (*deleter)(const Slice&, void *value);

	LRUHandle* next;
	LRUHandle* prev;
	
};

class LRUCache{
public:
	
	
	void SetCapacity(size_t capacity) {capacity_=capacity; }

private:
	size_t capacity_;
	
	size_t usage_;
	
	LRUHandle lru_;
	
	LRUHandle in_use_;
};

LRUCache::LRUCache()
	:usage_(0)
{
	lru_.next=&lru_;
	lru_.prev=&lru_;
	in_use_.next=&in_use_;
	in_use_.prev=&in_use_;
}

Cache* NewLRUCache(size_t capacity)
{
	return new ShardedLRUCache(capacity);
}

}