
strutct FileMetaData{
	int refs;
	int allowed_seeks; // compaction前允许的seek次数
	uint64_t number;
	uint64_t file_size; // 文件字节数
	InternalKey smallest; // 表中的最小key
	InternalKey largest; // 
	
	FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}
};

class VersionEdit{

public:
	VersionEdit() { Clear(); }
	~VersionEdit() {}
	
	void Clear();
	
	void SetComparatorName(const Slice& name)
	{
		has_comparator_=true;
		comparator_=name.ToString();
	}
	
	void SetLogNumber(uint64_t num)
	{
		has_log_number_=true;
		log_number_=num;
	}
	void SetNextFile(uint64_t num) {
		has_next_file_number_ = true;
		next_file_number_ = num;
	}
	void SetLastSequence(SequenceNumber seq)
	{
		has_last_sequence_=true;
		last_sequence_=seq;
	}
	
	void EncodeTo(std::string* dst) const;
	
	Status DecodeFrom(const Slice& src);
	
private:

	typedef std::set<std::pair<int, uint64_t> > DeletedFileSet;

	std::string comparator_; //  比较用户key的comparator的名字
	uint64_t log_number_;
	uint64_t prev_log_number_;
	uint64_t next_file_number_;
	SequenceNumber last_sequence_; // uint64_t
	bool has_comparator_;
	bool has_log_number_;
	bool has_prev_log_number_;
	bool has_next_file_number_;
	bool has_last_sequence_;
	
	std::vector<std::pair<int, InternalKey> > compact_pointers_;
	DeletedFileSet deleted_files;
	std::vector<std::pair<int, FileMetaData> > new_files_;
};