
namespace leveldb{

namespace log{

// 预先计算所有记录类型的crc32c值
static void InitTypeCrc(uint32_t *type_crc)
{
	for(int i=0; i<=kMaxRecordType; i++)
	{
		char t=static_cast<char>(i);
		type_crc[i]=crc32c::Value(&t, 1);
	}
}

Writer::Writer(WritableFile* dest)
	:dest_(dest),
	block_offset_(0){
	InitTypeCrc(type_crc_);
}

// 将字符串slice写到记录文件，控制每次写不超过kBlockSize
Status Writer::AddRecord(const Slice& slice)
{
	const char* ptr=slice.data();
	size_t left=slice.size();
	
	Status s;
	bool begin=true;
	do{
		const int leftover=kBlockSize - block_offset_;
		assert(leftover >=0);
		if(leftover < kHeaderSize) // 使用新block,在剩余长度不足kHeaderSize的旧block中填空字符\0
		{
			if(leftover > 0)
			{
				assert(kHeaderSize ==7);
				dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
			}
			block_offset_=0;
		}
		
		assert(kBlockSize -block_offset_ - kHeaderSize >=0);
		
		const size_t avail=kBlockSize - block_offset_ - kHeaderSize;
		const size_t fragment_length= (left < avail)  ?  left  :  avail; // 此次写的长度
		
		RecordType type;
		const bool end=(left == fragment_length); // 本次写完
		if(begin  &&  end)
			type=kFullType;
		else if(begin)
			type=kFirstType;
		else if(end)
			type=kLastType;
		else
			type=kMiddleType;
		
		s=EmitPhysicalRecord(type, ptr, fragment_length);
		ptr += fragment_length;
		left -= fragment_length;
		begin =false;		
	}while(s.ok()  &&  left>0);
	return s;
}

// 写类型t的从ptr开始的n字节,写到物理文件dest_中
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n)
{
	assert( n<=0xffff); // 保证长度可以写到两个字节里
	assert(block_offset_ + kHeaderSize + n <= kBlockSize);
	
	char buf[kHeaderSize];
	buf[4]=static_cast<char>(n & 0xff);
	buf[5]=static_cast<char>(n  >>  8);
	buf[6]=static_cast<char>(t);
	
	uint32_t crc=crc32c::Extend(type_crc_[t], ptr, n);
	crc==crc32c::Mask(crc);
	EncodeFixed32(buf, crc);
	
	Status s=dest_->Append(Slice(buf, kHeaderSize)); // 4字节checksum, 2字节length，1字节type
	if(s.ok())
	{
		s=dest_->Append(Slice(ptr, n));
		if(s.ok())
			s=dest_->Flush(); // 刷新dest_缓存，写到物理文件中
	}
	block_offset_ += kHeaderSize +n;
	return s;
}

}
}