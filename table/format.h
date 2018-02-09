

// BlockHandle指向存储数据块或者meta块的文件的内容
class BlockHandle{
	
	void EncodeTo(std::string* dst) const;
	
	uint64_t offset_; // block在文件中偏移量
	uint64_t size_;
};