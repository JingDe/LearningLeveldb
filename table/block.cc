
inline uint32_t Block::NumRestarts() const
{
	assert(size_ >= sizeof(uint32_t));
	return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

Block::Block(const BlockContents& contents)
	:data_(contents.data.data()),
	size_(contents.data.size()),
	owned_(contents.heap_allcated)
{
	if(size_ < sizeof(uint32_t))
		size_ = 0;
	else
	{
		size_t max_restarts_allowed=(size-sizeof(uint32_t)) / sizeof(uint32_t);
		if(NumRestarts() > max_restarts_allowed)
			size_ = 0;
		else
			restart_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t); // restart数组开始偏移位置
	}
}

// 解码下一个块entry，开始于p，存储共享key的字节数、不共享的字节数、value的长度
// limit之后不解引用
static inline const char* DecodeEntry(const char* p, const char* limit, 
							uint32_t* shared, uint32_t* non_shared, uint32_t* value_length)
{
	if(limit-p < 3)
		return NULL;
	*shared=reinterpret_cast<const unsigned char*>(p)[0];
	*non_shared=reinterpret_cast<const unsigned char*>(p)[1];
	*value_length=reinterpret_cast<const unsigned char*>(p)[2];
	if((*shared  |  *non_shared  |  *value_length) < 128)
	{ // 三个值编码都编码为一个字节
		p += 3;
	}
	else
	{
		if((p=GetVarint32Ptr(p, limit, shared))==NULL)  return NULL;
		if ((p = GetVarint32Ptr(p, limit, non_shared)) == NULL) return NULL;
		if ((p = GetVarint32Ptr(p, limit, value_length)) == NULL) return NULL;
	}
	
	if(static_cast<uint32_t>(limit-p) < (*non_shared + *value_length))
		return NULL;
	return p;
}

class Block::Iter : public Iterator
{
	const Comparator* const comparator_;
	const char* const data_; // 底层块内容
	uint32_t const restarts_; // restart数组的偏移量，fixed32数组
	uint32_t const num_restarts_; // restart数组的元素个数
	
	uint32_t current_; // 当前entry在data_中的偏移位置，如果!Valid大于等于restarts_
	uint32_t restart_index_; // current_所在的restart块的下标
	std::string key_;
	Slice value_;
	Status status_;
	
	// 返回当前entry之后的位置
	inline uint32_t NextEntryOffset() const
	{
		return (value_.data() + value_.size()) - data_;
	}
	
	// 指向第index个重启点
	void SeekToRestartPoint(uint32_t index)
	{
		key_.clear();
		restart_index_ = index;
		
		uint32_t offset=GetRestartPoint(index);
		value_ = Slice(data_ + offset, 0);
	}
	
public:	
	Iter(const Comparator* comparator,
		const char* data,
		uint32_t restarts,
		uint32_t num_restarts)
        :comparator_(comparator),
        data_(data), // block数据
        restarts_(restarts), // restart数组位置
        num_restarts_(num_restarts), // restart数组长度
        current_(restarts_), 
        restart_index_(num_restarts_) // 
	{
		assert(num_restarts_ > 0);
	}
	
	// 获得第index个重启点在data_中的位置
	uint32_t GetRestartPoint(uint32_t index) 
	{
		assert(index < num_restarts_);
		return DecodeFixed32(data_ + restarts_ + index*sizeof(uint32_t));
	}
	
	virtual void Seek(const Slice& target)
	{
		// 在restart数组中二分查找，查找最后一个有key小于target的重启点
		uint32_t left=0;
		uint32_t right=num_restarts_ -1;
		while(left < right)
		{
			uint32_t mid =(left+right+1) /2; // +1：偶数个数时，取中间偏右位置
			uint32_t region_offset = GetRestartPoint(mid); // mid个重启位置
			uint32_t shared, non_shared, value_length;			
			const char* key_ptr=DecodeEntry(data_ + region_offset, data_+restarts_, 
											&shared, &non_shared, &value_length);
			if(key_ptr==NULL  ||  (shared!=0)
			{
				CorruptionError();
				return;
			}
			Slice mid_key(key_ptr, non_shared); // key_ptr 指向非共享key
			if(Compare(mid_key, target) < 0)
				left=mid; // mid处的key小于target，mid之前的块不考虑
			else
				right=mid-1; // mid处的key大于target，mid和mid之后的块不考虑
		}
		
		SeekToRestartPoint(left);
		while(true)
		{
			if(!ParseNextKey())
				return;
			
		}
	}
	
private:
	void CorruptionError()
	{
		current_=restarts_;
		restart_index_=num_restarts_;
		status_ = Status::Corruption("bad entry in block");
		key_.clear();
		value_.clear();
	}
	
	bool ParseNextKey()
	{
		current_=NextEntryOffset();
		const char* p=data_ + current_;
		const char* limit=data_ + restarts_;
		if(p >= limit)
		{
			current_=restarts_;
			restart_index_=num_restarts_;
			return false;
		}
		
		uint32_t shared, non_shared, value_length;
		p=DecodeEntry(p, limit, &shared, &non_shared, &value_length);
		
	}
};

Iterator* Block::NewIterator(const Comparator* cmp)
{
	if(size_ < sizeof(uint32_t))
		return NewErrorIterator(Status::Corruption("bad block contents"));
	const uint32_t num_restarts = NumRestarts();
	if(num_restarts == 0)
		return NewEmptyIterator();
	else
		return new Iter(cmp, data_, restart_offset_, num_restarts);
}