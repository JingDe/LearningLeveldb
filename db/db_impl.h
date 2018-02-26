
class DBImpl : DB{

private:

	Env* const env_;
	const InternalKeyComparator internal_comparator_;
	const InternalFilterPolicy internal_filter_policy_;
	const Options options_;
	
	TableCache* table_cache_;
	
	FileLock* db_lock_;
	
	port::Mutex mutex_;
	MemTable* mem_;
    MemTable* imm_; // 正被compact的memtable
	
	WritableFile* logfile_;
	uint64_t logfile_number_;
	log::Writer* log_;
	
	std::set<uint64_t> pending_outputs_; // 保护的table文件，例如正在进行压缩
	
	struct ManualCompaction{
		int level;
		bool done;
		const InternalKey* begin;
		const InternalKey* end;
		InternalKey tmp_storage; // 跟踪compaction过程
	};
	ManualCompaction* manual_compaction_;
	
	VersionSet* versions_;
	
	Status bg_error_;
	
	// 每一层的compaction统计
	// stats_[level]存储为level层产生数据的compaction统计
	struct CompactionStats{
		int64_t micros;
		int64_t bytes_read;
		int64_t bytes_written;
		
		void Add(const CompactionStats& c)
		{
			this->micros += c.micros;
			this->bytes_read += c.bytes_read;
			this->bytes_written += c.bytes_written;
		}
	};
	CompactionStats stats_[config::kNumLevels];
	
	
	const Comparator* user_comparator() const
	{
		return internal_comparator_.user_comparator();
	}
};