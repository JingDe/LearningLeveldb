


TEST(Coding, Fixed32)
{
	std::string s;
	for(uint32_t v=0; v<100000; v++)
		PutFixed32(&s, v);
	
	const char* p=s.data();
	for(uint32_t v=0; v<100000; v++)
	{
		uint32_t actual=DecodeFixed32(p);
		ASSERT_EQ(v, actual);
		p+=sizeof(uint32_t);
	}
}

TEST(Coding, Varint32)
{
	std::string s;
	for(uint32_t i=0; i< (32*32); i++) // i从0到 2^10-1
	{
		uint32_t v=(i/32) << (i % 32); // v是i的右边5位置0
		PutVarint32(&s, v); // 将v 每7位一组编码成字符append到s
	}
	
	const char* p=s.data();
	const char* limit=p+s.size();
	for(uint32_t i=0; i< (32*32); i++)
	{
		uint32_t expected=(i/32) << (i % 32);
		uint32_t actual;
		const char* start=p;
		p=GetVarint32Ptr(p, limit, &actual); // 从p-limit解码获得actual
		ASSERT_TRUE(p!=NULL);
		ASSERT_EQ(expected, actual);
		ASSERT_EQ(VarintLength(actual), p-start);
	}
}

TEST(Coding, Varint64)
{
	std::vector<uint64_t> values;
	values.push_back(0);
	values.push_back(100);
	values.push_back(~static_cast<uint64_t>(0)); // 按位取反
	values.push_back(~static_cast<uint64_t>(0)-1); 
	for(uint32_t k=0; k<64; k++)
	{
		const uint64_t power=1ull  <<  k; // power是右边数第k位是1，其余为0
		values.push_back(power);
		values.push_back(power-1);
		values.push_back(power+1);
	}
	
	std::string s;
	for(size_t i=0; i<values.size(); i++)
		PutVarint64(&s, values[i]);
	
	const char* p=s.data();
	const char* limit=p+s.size();
	for(size_t i=0; i<values.size(); i++)
	{
		ASSERT_TRUE(p < limit);
		uint64_t actual;
		const char* start=p;
		p=GetVarint64Ptr(p, limit, &actual); // 从p-limit解码获得actual
		ASSERT_TRUE(p !=NULL);
		ASSERT_EQ(values[i], actual);
		ASSERT_EQ(VarintLength(actual), p-start);
	}
	ASSERT_EQ(p, limit);
}
