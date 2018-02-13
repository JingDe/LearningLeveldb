

class Block{
	
	explicit Block(const BlockContents& contents);
	
	Iterator* NewIterator(const Comparator* comparator);
	
	const char* data_;
    size_t size_; // block的大小（不包括CompressionType或crc)
	uint32_t restart_offset_; // restart数组在data_中的偏移位置
	bool owned_; // Block是否拥有data_
	
	class Iter;
};