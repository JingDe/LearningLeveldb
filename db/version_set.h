
class Version {

	void Unref();

	VersionSet* vset_;
	Version* next_;
	Version* prev_;
	
	int refs_; // 指向这个Version的引用
	
	 
	FileMetaData* file_to_compact_; // 将compact的文件
	int file_to_compact_level_;
	
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
	
	Status Recover(bool *save_manifest); // 从文件中恢复最近保存的 descriptor
	
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
	log::Writer* descriptor_log_;
	Version dummy_versions_; // version链表头
	Version* current_; // dummy_versions_.prev_
};