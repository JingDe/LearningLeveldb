
class EnvPosixTestHelper{
	
	// 设置将被打开的只读文件的最大数量
	static void SetReadOnlyFDLimit(int limit);
	
	// 将被通过mmap映射的只读文件的个数
	static void SetReadOnlyMMapLimit(int limit);
};