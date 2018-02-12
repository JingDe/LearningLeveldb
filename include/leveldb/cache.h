
namespace leveldb{
	
Cache* NewLRUCache(size_t capacity);

class Cache{
public:
	
	struct Handle { };
	
	// 返回对应key的handle，调用方负责调用this->Release()
	virtual Handle* Lookup(const Slice& key) = 0;
};

}