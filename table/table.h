
class Table{
	
	// 尝试打开table，table存储在文件file中。读取元数据以便从table中读取数据
	// 若成功，返回ok，设置*table指向新打开的table。用户不使用*table时需要delete *table。
	// 如果初始化table出错，*table设为NULL。
	// 用户必须确保table的生命周期中source存在。
	// 当table被使用中，*file必须存在
	static Status Open(const Options& options,
                     RandomAccessFile* file,
                     uint64_t file_size,
                     Table** table);
					 
private:
	struct Rep;
	Rep* rep_;
	
	explicit Table(Rep* rep) { rep_ = rep; }
	
	void ReadMeta(const Footer& footer);
	
};