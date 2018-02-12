
class TableBuilder{
	// 创建builder，存储正在building的table的内容在file中
	// 调用者负责关闭file
	TableBuilder(const Options& options, WritableFile* file);
	
	Status status() const;
	
	bool ok() const { return status().ok(); }
	
	// 返回目前为止生成的文件的大小。如果在Finish()之后调用，返回最后生成的文件的大小
	uint64_t FileSize() const;
	
	struct Rep;
	Rep* rep_;
};