
extern void PutVarint32(std::string* dst, uint32_t value);

extern bool GetLengthPrefixedSlice(Slice* input, Slice* result);

extern const char* GetVarint32PtrFallback(const char* p, const char* limit, uint32_t* value);

// 从字符串p-limit解码获得value
inline const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value)
{
	if(p< limit)
	{
		uint32_t result=*(reinterpret_cast<const unsigned char*>(p));
		if((result & 128)==0) // result的第8位为0, result是编码前 最后一个7位串
		{
			*value=result;
			return p+1;
		}
	}
	return GetVarint32PtrFallback(p, limit, value); 
}

inline uint32_t DecodeFixed32(const char* ptr)
{
	if(port::kLittleEndian)
	{
		uint32_t result;
		memcpy(&result, ptr, sizeof(result));
		return result;
	}
	else
	{
		return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
				| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
				| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) <<16)
				| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24);
	}
}

inline uint64_t DecodeFixed64(const char* ptr)
{
	if(port::kLittleEndian)
	{
		uint64_t result;
		memcpy(&result, ptr, sizeof(result));
		return result;
	}
	else
	{
		uint64_t lo=DecodeFixed32(ptr);
		uint64_t hi=DecodeFixed32(ptr+4);
		return (hi << 32) | lo;
	}
}