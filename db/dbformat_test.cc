
static std::string IKey(const std::string& user_key, uint64_t seq, ValueType vt)
{
	std::string encoded;
	AppendInternalKey(&encoded, ParsedInternalKey(user_key, seq, vt));
	return encoded;
}

static void TestKey(const std::string& key, uint64_t seq, ValueType vt)
{
	std::string encoded=IKey(key, seq, vt);
	
	Slice in(encoded);
	ParsedInternalKey decoded("", 0, kTypeValue);
	
	
}