
MemTable::MemTable(const InternalKeyComparator& cmp)
	:comparator_(cmp),
	refs_(0),
	table_(comparator_, &arena_)
{
	
}

// internal_key_size + key + [SequenceNumber+ValueType(8字节)] + val_size + value
// internal key : key+ValueType
void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key, const Slice& value)
{
	size_t key_size=key.size();
	size_t val_size=value.size();
	size_t internal_key_size=key_size+8;
	const size_t encoded_len=VarintLength(internal_key_size) + internal_key_size + VarintLength(val_size)+val_size;
	char* buf=arena_.Allocate(encoded_len);
	char* p=EncodeVarint32(buf, internal_key_size);
	memcpy(p, key.size(), key_size);
	p += key_size;
	EncodeFixed64(p, (s<<8) | type);
	p += 8;
	p = EncodeVarint32(p, val_size);
	memcpy(p, value.data(), val_size);
	assert( (p+val_size) - buf == encoded_len);
	table_.Insert(buf);
}

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr) const
{
	Slice a=G
}

size_t MemTable::ApproximateMemoryUsage()
{
	return arena_.MemoryUsage();
}