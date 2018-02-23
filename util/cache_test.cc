
static std::string EncodeKey(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}

static int DecodeKey(const Slice& k)
{
	assert(k.size() == 4);
	return DecodeFixed32(k.data());
}

static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }

static int DecodeValue(void* v)
{
	return reinterpret_cast<uintptr_t>(v);
}

class CacheTest{
public:
	static CacheTest* current_;
	
	static void Deleter(const Slice& key, void* v)
	{
		current_->deleted_keys_.push_back(DecodeKey(key));
		current_->deleted_values_.push_back(DecodeValue(v));
	}
	
	static const int kCacheSize=1000;
	std::vector<int> deleted_keys_;
	std::vector<int> deleted_values_;
	Cache* cache_;
	
	CacheTest():cache_(NewLRUCache(kCacheSize))
	{
		current_=this;
	}
	
	~CacheTest(){
		delete cache_;
	}
	
	int Lookup(int key)
	{
		Cache::Handle* handle=cache_->Lookup(EncodeKey(key));
		const int r=(handle==NULL) ? -1 : DecodeValue(cache_->Value(handle));
		if(handle !=NULL)
			cache_->Release(handle);
		return r;
	}
	
	void Insert(int key, int value, int charge=1)
	{
		cache_->Release(cache_->Insert(EncodeKey(key), EncodeValue(value), charge, &CacheTest::Deleter));
	}
	
	Cache::Handle* InsertAndReturnHandle(int key, int value, int charge=1)
	{
		return cache_->Insert(EncodeKey(key), EncodeValue(value), charge, &CacheTest::Deleter);
	}
};
CacheTest* CacheTest::current_;


