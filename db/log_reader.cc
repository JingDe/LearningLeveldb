
Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0), // 最后读取的完整记录的开始的块的位置
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0)
	{}	  

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
	ReportDrop(bytes, Status::Corruption(reason));
}
	
bool Reader::ReadRecord(Slice* record, std::string* scratch)
{
	if(last_record_offset_ < initial_offset_) // 最近ReadRecord读取记录的位置 < buffer_之后的位置
	{
		if(!SkipToInitialBlock())
			return false;
	}
	
	scratch->clear();
	record->clear();
	bool in_fragmented_record=false;
	uint64_t prospective_record_offset=0;// 当前读取的逻辑记录（即完整记录）的偏移位置
	
	Slice fragment;
	while(true)
	{
		// 读 initial_offset_ 后的第一条记录，去掉记录头返回给fragment
		const unsigned int record_type=ReadPhysicalRecord(&fragment);
		// 物理记录的偏移位置
		uint64_t physical_record_offset=end_of_buffer_offset_ - buffer.size() - kHeaderSize - fragment.size();
		
		if(resyncing_)
		{
			if(record_type == kMiddleType)
				continue;
			else if(record_type == kLastType)
			{
				resyncing_ = false;
				continue;
			}
			else // kFullType  kFirstType
				resyncing_ =false;
		}
		
		switch(record_type)
		{
			cast kFullType:
				if(in_fragmented_record)
				{
					if(scratch->empty())
						in_fragmented_record=false;
					else
						ReportCorruption(scratch->size(), "partial record without end(1)");
				}
				prospective_record_offset = physical_record_offset;
				scratch->clear();
				*record=fragment;
				last_record_offset_ = prospective_record_offset;
				return true;
			case kFirstType:
				if(in_fragmented_record)
				{
					if(scratch->empty())
						in_fragmented_record=false;
					else
						ReportCorruption(scratch->size(), "partial record without end(2)");
				}
				prospective_record_offset=physical_record_offset;
				scratch->assign(fragment.data(), fragment.size()); // scratch暂存kFirstType部分数据
				in_fragmented_record=true; // 标记当前处理一条分割多个块的记录
				break;
			case kMiddleType:
				if(!in_fragmented_record)
					ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
				else
					scratch->apend(fragment.data(), fragment.size());
				break;
			case kLastType:
				if(!in_fragmented_record)
					ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
				else
				{
					scratch->append(fragment.data(), fragment.size());
					*record=Slice(*scratch);
					last_record_offset_=prospective_record_offset; // 读到kFirstType时的记录位置
					return true;
				}
				break;
			case kEof:
				if(in_fragmented_record)
					scratch->clear();
				return false;
			case kBadRecord:
				if(in_fragmented_record)
				{
					ReportCorruption(scratch->size(), "error in middle of record");
					in_fragmented_record=false;
					scratch->clear();
				}
				break;
			default:
			{
				char buf[40];
				snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
				ReportCorruption((fragment.size()+(in_fragmented_record ? scratch.size() : 0)), buf);
				in_fragmented_record=false;
				scratch->clear();
				break;
			}
		}
	}
	return false;
}

bool Reader::SkipToInitialBlock() 
{
	// 定位包含第一条记录的块开始的位置block_start_location
	size_t offset_in_block = initial_offset_ % kBlockSize; // initial_offset_是文件的初始偏移位置
		// offset_in_block 是该偏移位置对应在块中的偏移位置
	uint64_t block_start_location = initial_offset_ - offset_in_block; // 相减获得初始偏移位置所在的块的开始位置
	
	if(offset_in_block > kBlockSize -6) // 下一个块
	{
		offset_in_block=0;
		block_start_location += kBlockSize;
	}		
	
	end_of_buffer_offset_ = block_start_location;
	
	if(block_start_location > 0) // 文件跳到包含第一条记录的块的开始位置
	{
		Status skip_status = file_->Skip(block_start_location);
		if(!skip_status.ok())
		{
			ReportDrop(block_start_location, skip_status);
			return false;
		}
	}
	return true; // block_start_location == 0
}

unsigned int Reader::ReadPhysicalRecord(Slice* result)
{
	while(true)
	{
		// 已读数据不足一个记录头，读取一条记录
		if(buffer_.size() < kHeaderSize)
		{
			if(!eof_) // eof_为false
			{
				buffer_.clear();
				Status status=file_->Read(kBlockSize, &buffer_, backing_store_); 
					// 读最多kBlockSize字节到buffer_，backing_store_是临时存储
				end_of_buffer_offset_ += buffer_.size();
				if(!status.ok())
				{
					buffer_.clear();
					ReportDrop(kBlockSize, status);
					eof_=true;
					return kEof;
				}
				else if(buffer_.size() < kBlockSize) // 读到最后一条记录
					eof_=true;
				continue;
			}
			else
			{
				// 如果buffer_不为空，表示文件末尾有一个截断的文件头，可能是因为writer在写记录头过程中崩溃，忽略不报错，看做EOF
				buffer_.clear();
				return kEof;
			}
		}
		
		// 获得一条记录
		// 解析记录头：4字节checksum, 2字节length，1字节RecordType
		const char* header=buffer_.data();
		const uint32_t a=static_cast<uint32_t>(header[4]) & 0xff;
		const uint32_t b=static_cast<uint32_t>(header[5]) & 0xff;
		const unsigned int type=header[6];
		const uint32_t length= a | (b << 8);
		if(kHeaderSize + length > buffer_.size())
		{
			size_t drop_size=buffer_.size();
			buffer_.clear();
			if(!eof_) // 文件中间的错误记录
			{
				ReportCorruption(drop_size, "bad record length");
				return kBadRecord;
			}
			// 文件结尾处读到了长度小于 length 的记录，认为writer中途崩溃，不报错返回EOF
			return kEof;
		}
		
		if(type ==kZeroType  &&  length==0)
		{
			buffer_.clear();
			return kBadRecord;
		}
		
		if(checksum_)
		{
			uint32_t expected_crc=crc32c::Unmask(DecodeFixed32(header));
			uint32_t actual_crc=crc32c::Value(header+6, 1+length);
			if(actual_crc != expected_crc)
			{
				size_t drop_size = buffer_.size();
				buffer_.clear();
				ReportCorruption(drop_size, "checksum mismatch");
				return kBadRecord;
			}
		}
		
		buffer_.remove_prefix(kHeaderSize + length); // 移动到下一条记录开头位置，只移动Slice指针，不释放内存(header)
		
		// 跳过 initial_offset_ 之前的记录
		if(end_of_buffer_offset_ - buffer_.size() - kHeaderSize -length < initial_offset_)
		{
			result->clear();
			return kBadRecord;
		}
		
		*result=Slice(header+kHeaderSize, length);
		return type;
	}
}