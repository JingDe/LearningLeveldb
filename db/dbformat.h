
namespace leveldb{
	
typedef uint64_t SequenceNumber;

class InternalKeyComparator: public Comparator{
private:
	const Comparator* user_comparator;

public:
	const Comparator* user_comparator() const {
		return user_comparator_;
	}
};

class InternalKey{
private:
	std::string rep_;
	
public:
	Slice Encode() const
	{
		assert(!rep_.empty());
		return rep_;
	}
};

}