
BlockBuilder::BlockBuilder(const Options* options)
	:options_(options),
	restarts_(),
	counter_(0),
	finished_(false)
{
	assert(options->block_restart_interval >=1);
	restarts_.push_back(0);// 第一个重启点
}

void BlockBuilder::Reset()
{
	buffer_.clear();
	restarts_.clear();
	restarts_.push_back(0);
	counter_=0;
	finished_=false;
	last_key_.clear();
}

// 原始数据缓存 + 重启点数组 + 重启点数组长度
size_t BlockBuilder::CurrentSizeEstimate() const
{
	return (buffer_.size() + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t));
}

// <shared_size><non_shared_size><value_size><shared><non_shard><value>
void BlockBuilder::Add(const Slice& key, const Slice& value)
{
	Slice last_key_piece(last_key_);
	assert(!finished_); // 没有调用Finish
	assert(counter_ <= options_->block_restart_interval); // 没到新的重启点
	assert(buffer_.empty()  ||  options_->comparator->Compare(key, last_key_piece) >0); // 新添加key必须大于最后添加的key
	size_t shared=0;
	if(counter_ < options_->block_restart_interval)
	{
		// 找到key与之前last_key_共用的长度shared
		const size_t min_length=std::min(last_key_piece.size(), key.size());
		while((shared<min_length)  &&  (last_key_piece[shared]==key[shared]))
			shared++;
	}
	else
	{
		restarts_.push_back(buffer_.size()); // 新的重启点，buffer_累加block的内容
		counter_=0;
	}
	const size_t non_shared=key.size()-shared;
	
	// 添加"<shared><non_shared><value_size>" to buffer_
	PutVarint32(&buffer_, shared);
	PutVarint32(&buffer_, non_shared);
	PutVarint32(&buffer_, value.size());
	
	buffer_.append(key.data() + shared, non_shared); // buffer_添加key的不共用部分
	buffer_.append(value.data(), value.size());
	
	last_key_.resize(shared); // 更新last_key_为key
	last_key_.append(key.data() +shared, non_shared);
	assert(Slice(last_key_)==key);
	counter_++;
}

Slice BlockBuilder::Finish()
{
	for(size_t i=0; i<restarts_.size(); i++)
		PutFixed32(&buffer_, restarts[i]);
	PutFixed32(&buffer_, restarts_.size());
	finished_=true;
	return Slice(buffer_);
}