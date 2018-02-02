
namespace leveldb{

namespace log{

class Writer{
public:
	explicit Writer(WritableFile* dest);
	
	Status AddRecord(const Slice& slice);
	
private:
	WritableFile* dest_;
	int block_offset_;
	
	uint32_t type_crc[kMaxRecordType+1]; // 预先计算所有记录类型的crc32c值
	
};

}
	
}