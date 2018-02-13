

// BlockHandle指向存储数据块或者meta块的文件的内容
class BlockHandle{
	
	void EncodeTo(std::string* dst) const;
	void set_offset(uint64_t offset) { offset_ = offset; }
	void set_size(uint64_t size) { size_ = size; }
	uint64_t size() const { return size_; }
	
	enum { kMaxEncodedLength = 10 + 10 };// BlockHandle的最大编码长度
	
	uint64_t offset_; // block在文件中偏移量
	uint64_t size_; // block的大小，不包括CompressionType和crc
};

class Footer{
	
	void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }
	
	const BlockHandle& index_handle() const {
		return index_handle_;
	}
	
	void EncodeTo(std::string* dst) const;
	
	// 两个BlockHandle和一个magic number
	enum {
		kEncodedLength = 2*BlockHandle::kMaxEncodedLength + 8
	};
	
	BlockHandle metaindex_handle_;
	BlockHandle index_handle_;
};

static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

struct BlockContents
{
	Slice data;
	bool cachable;
	bool heap_allocated; // false表示data指向block数据由底层文件实现提供指针，不可能缓存
						 // true表示在堆上分配的，可以缓存
};

// 读取file中handle表示的块，通过result返回
extern Status ReadBlock(RandomAccessFile* file,
                        const ReadOptions& options,
                        const BlockHandle& handle,
                        BlockContents* result);