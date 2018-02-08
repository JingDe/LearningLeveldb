
MemTable::MemTable(const InternalKeyComparator& cmp)
	:comparator_(cmp),
	refs_(0),
	table_(comparator_, &arena_)
{
	
}

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr) const
{
	Slice a=G
}