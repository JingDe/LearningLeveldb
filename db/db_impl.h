
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
	
	const Comparator* user_comparator() const
	{
		return internal_comparator_.user_comparator();
	}
};