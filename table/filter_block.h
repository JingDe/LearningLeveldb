

// FilterBlockBuilder 用来对特定Table构建所有的filter
// 生成一个string，存储为Table的一个特殊block
// 调用FilterBlockBuilder的顺序必须满足regexp: (StartBlock AddKey*)* Finish
class FilterBlockBuilder{
	
	void StartBlock(uint64_t block_offset);
	void AddKey(const Slice& key);
	
	const FilterPolicy* policy_;
	std::string keys_; // 所有的key
	std::vector<size_t> start_; // 每个key在keys_中开始位置
	std::string result_; // 当前计算的filter数据
	std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument
	std::vector<uint32_t> filter_offsets_; // 每一个filter数据在results_中的开始位置
};

class FilterBlockReader{
	
	// 要求contents和policy必须在*this存在时存在
	FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
	
	const FilterPolicy* policy_;
	const char* data_;    // 指向filter数据
	const char* offset_;  // 指向offset数组
	size_t num_;          // offset数组的entry数
	size_t base_lg_; 	  // 编码参数
};