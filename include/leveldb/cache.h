
namespace leveldb{
	
Cache* NewLRUCache(size_t capacity);

class Cache{
public:
	
	struct Handle { };
	
	// 返回对应key的handle，调用方负责调用this->Release()
	virtual Handle* Lookup(const Slice& key) = 0;
	
	// 返回数字id，可以被多个用户共享相同的缓存来划分key空间
	// 用户在开始阶段分配一个id，在缓存的key之前加上id
	virtual uint64_t NewId() = 0;
};

}