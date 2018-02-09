
class DBImpl : DB{

private:

	Env* const env_;
	const InternalKeyComparator internal_comparator_;
	const InternalFilterPolicy internal_filter_policy_;
	const Options options_;
	
	TableCache* table_cache_;
	
	FileLock* db_lock_;
	
	port::Mutex mutex_;
	
	VersionSet* version_;
	
	std::set<uint64_t> pending_outputs_; // 保护的table文件，l例如正在进行压缩
	
	const Comparator* user_comparator() const
	{
		return internal_comparator_.user_comparator();
	}
};