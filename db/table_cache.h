
class TableCache{
	
	
	// 返回一个对指定文件的迭代器，文件长度是file_size，
	// 如果tableptr非空，设置tableptr指向返回迭代器底层的Table对象
	// 返回的*tableptr对象属于cache，不能被delete，在返回迭代器有效期间一直存在
	Iterator* NewIterator(const ReadOptions& options,
                        uint64_t file_number,
                        uint64_t file_size,
                        Table** tableptr = NULL);
	
	Cache* cache_; // key是文件序号
	
	Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);
};