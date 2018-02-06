
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