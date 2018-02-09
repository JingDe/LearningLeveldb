

// FilterBlockBuilder 用来对特定Table构建所有的filter
// 生成一个string，存储为Table的一个特殊block
// 调用FilterBlockBuilder的顺序必须满足regexp: (StartBlock AddKey*)* Finish
class FilterBlockBuilder{
	
	void AddKey(const Slice& key);
	
	std::string keys_; // 所有的key
	std::vector<size_t> start_; // 每个key在keys_中开始位置
};