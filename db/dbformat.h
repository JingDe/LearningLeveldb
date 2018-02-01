
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

}