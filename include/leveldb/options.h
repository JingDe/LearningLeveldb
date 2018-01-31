
struct Options{

	const Comparator* comparator;
	
	bool create_if_missing;
	
	Env* env;
	
	Logger* info_log;
	
	Cache* block_cache;
	
	const FilterPolicy* filter_policy;
};