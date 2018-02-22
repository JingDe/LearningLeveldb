
static uint64_t PackSequenceAndType(uint64_t seq, ValueType t)
{
	assert(seq <= kMaxSequenceNumber);
	assert(t<= kValueTypeForSeek); // 最大的ValueType
	return (seq<<8)  |  t;
}

void AppendInternalKey(std::string* result, const ParseInternalKey& key)
{
	result->append(key.user_key.data(), key.user_key.size());
	PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

void InternalKeyComparator::FindShortestSeparator(std::string* start, const Slice& limit) const
{
	// 缩短key的用户部分
	Slice user_start=ExtraceUserKey(*start);
	Slice user_limit=ExtraceUserKey(limit);
	std::string tmp(user_start.data(), user_start.size());
	user_comparator_->FindShortestSeparator(&tmp, user_limit);
	if(tmp.size() < user_start.size()  &&  user_comparator_->Compare(user_start, tmp)<0)
	{
		PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek)); 
		// 为tmp添加最大的sequence number和ValueType
		assert(this->Compare(*start, tmp)< 0);
		assert(this->Compare(tmp, limit) <0);
		start->swap(tmp);
	}
}