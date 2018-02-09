
class BlockBuilder{
	
	void Add(const Slice& key, const Slice& value);
	
	size_t CurrentSizeEstimate() const; // 正在构建的块的未压缩的估计大小
	
	const Options*        options_;
	std::string buffer_; // 目的缓冲
	std::vector<uint32_t> restarts_; // 重启点
	int counter_; // 上一个重启点之后emit的条目个数,当超过options->block_restart_interval创建新的重启点
	bool finished_; // 是否调用Finish()
	std::string last_key_;
};