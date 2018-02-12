

// BlockHandle指向存储数据块或者meta块的文件的内容
class BlockHandle{
	
	void EncodeTo(std::string* dst) const;
	void set_offset(uint64_t offset) { offset_ = offset; }
	void set_size(uint64_t size) { size_ = size; }
	
	enum { kMaxEncodedLength = 10 + 10 };// BlockHandle的最大编码长度
	
	uint64_t offset_; // block在文件中偏移量
	uint64_t size_; // block的大小
};

class Footer{
	
	void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }
	
	void EncodeTo(std::string* dst) const;
	
	// 两个BlockHandle和一个magic number
	enum {
		kEncodedLength = 2*BlockHandle::kMaxEncodedLength + 8
	};
	
	BlockHandle metaindex_handle_;
	BlockHandle index_handle_;
};

static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;