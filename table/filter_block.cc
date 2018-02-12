
// 每2KB数据生成一个filter
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

// 某个block的偏移量block_offset，所处的filter_offsets_的位置filter_index
void FilterBlockBuilder::StartBlock(uint64_t block_offset)
{
	uint64_t filter_index=(block_offset / kFilterBase);
	assert(filter_index >= filter_offsets_.size();
	while(filter_index > filter_offsets_.size()) // 当前filter的个数 
		GenerateFilter();
}

void FilterBlockBuilder::AddKey(const Slice& key)
{
	Slice k=key;
	start_.push_back(keys_.size());
	keys_.append(k.data(), k.size());
}


void FilterBlockBuilder::GenerateFilter()
{
	const size_t num_keys=start_.size(); // 当前key的数量
	if(num_keys==0)
	{
		filter_offsets_.push_back(result_.size()); // 直接生成第一个filter
		return ;
	}
	
	start_.push_back(keys_.size()); // 添加一个key的位置
	tmp_keys_.resize(num_keys); // 提取keys_中的每个key
	for(size_t i=0; i<num_keys; i++)
	{
		const char* base=keys_.data() + start_[i];
		size_t length=start_[i+1] - start_[i];
		tmp_keys_[i]=Slice(base, length);
	}
	
	filter_offsets_.push_back(result_.size()); // 添加一个filter位置
	// 根据当前所有key，创建一个filter添加到result_
	policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_); 
	
	tmp_keys_.clear();
	keys_.clear(); // 清除当前所有key
	start_.clear();
}
