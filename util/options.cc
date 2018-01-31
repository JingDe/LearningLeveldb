
namespace leveldb{

Options::Options()
	:comparator(BytewiseComparator()),
	create_if_missing(false),
	filter_policy(NULL)
	
}