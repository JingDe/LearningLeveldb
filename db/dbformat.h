
namespace leveldb{
	
typedef uint64_t SequenceNumber;

static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) -1);

inline Slice ExtractUserKey(const Slice& internal_key)
{
	assert(internal_key.size() >= 8);
	return Slice(internal_key.data(), internal_key.size()-8);
}

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
	void DecodeFrom(const Slice& s)
	{
		rep_.assign(s.data(), s.size());
	}
};

}