
class MemTable{

	explicit MemTable(const InternalKeyComparator& comparator);
	
	void Ref() { ++refs_; }
	

	struct KeyComparator{
		const InternalKeyComparator comparator;
		explicit KeyComparator(const InternalKeyComparator& c): comparator(c){}
		int operator()(const char* a, const char* b） const;
	};
	typedef SkipList<const char*, KeyComparator> Table;
	
	KeyComparator comparator_;
	int refs_;
	Table table_;
};