
namespace leveldb{

Options::Options()
	:comparator(BytewiseComparator()),
	filter_policy(NULL)
	
}