
class DBImpl : DB{

private:

	Env* const env_;
	const InternalKeyComparator internal_comparator_;
	const InternalFilterPolicy internal_filter_policy_;
	const Options options_;
	
	
	FileLock* db_lock_;
	
	port::Mutex mutex_;
};