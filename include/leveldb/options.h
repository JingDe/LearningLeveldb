

// 数据库内容存储在一系列block中，每一个block包含一系列key-value对。
// 每一个block在存储到一个文件之前可以被压缩
enum CompressionType{
	kNoCompression =  0x0,
	kSnappyCompression = 0x1
};

struct Options{

	const Comparator* comparator;
	
	bool create_if_missing;
	
	Env* env;
	
	Logger* info_log;
	
	Cache* block_cache;
	
	const FilterPolicy* filter_policy;
};