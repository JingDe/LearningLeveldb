
namespace log{

class Reader{
	
	class Reporter{
		
		virtual void Corruption(size_t bytes, const Status& status) = 0;
	};
	
	Reader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset);
		 
	bool ReadRecord(Slice* record, std::string* scratch); // 度吓一跳记录到record，scratch是临时存储
		 
private:
	SequentialFile* const file_;
	Reporter* const reporter_;
	bool const checksum_;
	char* const backing_store_;
	Slice buffer_;
	bool eof_;   // 当Read()返回值小于kBlockSize表示EOF

	uint64_t last_record_offset_; // ReadRecord返回的最后一个记录的位置
	uint64_t end_of_buffer_offset_; // buffer_之后的第一个位置

	uint64_t const initial_offset_; // 寻找第一个记录的位置

	// True if we are resynchronizing after a seek (initial_offset_ > 0). In
	// particular, a run of kMiddleType and kLastType records can be silently
	// skipped in this mode
	bool resyncing_;
	
	enum{
		kEof = kMaxRecordType +1,
		kBadRecord = kMaxRecordType + 2
	};
};

}