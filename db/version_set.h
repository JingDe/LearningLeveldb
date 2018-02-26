
class Compaction{
	
	VersionEdit* edit() { return &edit_; }
	
	int num_input_files(int which) const { return inputs_[which].size(); }
	
	// 返回input文件
	FileMetaData* input(int which, int i) const { return inputs_[which][i]; }
	
	// 是否是一个trivial compaction，可以通过移动一个input文件到下一层实现，没有merge或者split
	bool IsTrivialMove() const;
	
private:
	
	Compaction(const Options* options, int level);
	
	int level_;
	uint64_t max_output_file_size_;
	Version* input_version_;
	VersionEdit edit_;
	
	// 每次compaction读level_和下一层的inputs
	std::vector<FileMetaData*> inputs_[2]; // 两个inputs集合
	
	std::vector<FileMetaData*> grandparents_;
};

class Version {

	void Unref();
	
	// 如果在level层的某些文件与[*smallest_user_key,*largest_user_key]部分重叠返回true
	// smallest_user_key==NULL表示小于DB中所有key的key
	bool OverlapInLevel(int level,
                      const Slice* smallest_user_key,
                      const Slice* largest_user_key);
					  
	// 返回应该在其中放置一个新的memtable compaction结果的层，
	// 这个结果覆盖了[smallest_user_key,largest_user_key]
	int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);
	

	VersionSet* vset_;
	Version* next_;
	Version* prev_;
	
	int refs_; // 指向这个Version的引用
	
	std::vector<FileMetaData*> files_[config::kNumLevels]; // 每层的文件列表
	
	// 根据seek数据的下一次compact文件
	FileMetaData* file_to_compact_; // 将compact的文件
	int file_to_compact_level_;
	
	// 下一次compact的层，和compaction评分
	double compaction_score_; // 小于1表示compaction不必须
	int compaction_level_;

	explicit Verison(VersionSet* vset)
		: vset_(vset), next_(this), prev_(this), refs_(0),
		file_to_compact_(NULL),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {
	}
};

class VersionSet{
public:
	VersionSet(const std::string& dbname, const Options* options, TableCache* table_cache, const InternalKeyComparator*);
	
	Status LogAndApply(VersionEdit* edit, port::Mutex* mu) EXCLUSIVE_LOCKS_REQUIRED(mu);
	  
	Status Recover(bool *save_manifest); // 从文件中恢复最近保存的 descriptor
	
	uint64_t NewFileNumber() { return next_file_number_++; }
	
	bool NeedsCompaction() const {
		Version* v = current_;
		return (v->compaction_score_ >= 1) || (v->file_to_compact_ != NULL);
	}
	
	// 
	struct LevelSummaryStorage{
		char buffer[100];
	};
	const char* LevelSummary(LevelSummaryStorage* scratch) const;
	
	
private:
	class Builder;
	
	Env* const env_;
	const std::string dbname_;
	const Options* const options_;
	TableCache* const table_cache_;
	const InternalKeyComparator icmp_;
	uint64_t next_file_number_;
	uint64_t manifest_file_number_;
	uint64_t last_sequence_;
	uint64_t log_number_;
	uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted
	
	WritableFile* descriptor_file_; // MANIFEST文件
	log::Writer* descriptor_log_; // 写 descriptor_file_
	Version dummy_versions_; // version链表头
	Version* current_; // dummy_versions_.prev_
	
	// 每一层在下一次compaction开始的key
	// 空字符串或者有效的InternalKey
	std::string compact_pointer_[config::kNumLevels];
};