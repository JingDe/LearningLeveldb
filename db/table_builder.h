
class TableBuilder{
	// 创建builder，存储正在building的table的内容在file中
	// 调用者负责关闭file
	TableBuilder(const Options& options, WritableFile* file);
	
	Status status() const;
	
	bool ok() const { return status().ok(); }
	
	struct Rep;
	Rep* rep_;
};