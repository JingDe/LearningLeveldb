
namespace leveldb{
	
namespace config{
	
	static const int kNumLevels = 7;
	
	// 一个新的compact过的memtable如果不产生重叠被push到的最大层数
	// push到2层，避免相对代价高的层0=>1的compaction，以及避免高代价的manifest文件操作
	// 不一直push到最高层，是因为如果相同的key空间正被重复overwrite，会导致大量的浪费的磁盘空间
	static const int kMaxMemCompactLevel = 2;
}
	
typedef uint64_t SequenceNumber;
static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) -1);

enum ValueType {
	kTypeDeletion = 0x0,
	kTypeValue = 0x1
};
// kValueTypeForSeek定义的ValueType应该在构建一个ParsedInternalKey对象来seek到一个特定的序列号被传递，
// 因为按照降序排列序列号，并且ValueType编码在内部key的序列号的低8位，所以使用最高的ValueType
static const ValueType kValueTypeForSeek = kTypeValue;




struct ParsedInternalKey{
	Slice user_key;
	SequenceNumber sequence;
	ValueType type;
	
	ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) { }
};

// 将key的序列化append到*result
extern void AppendInternalKey(std::string* result,
                              const ParsedInternalKey& key);

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
	InternalKey(const Slice& user_key, SequenceNumber s, ValueType t)
	{
		AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
	}

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