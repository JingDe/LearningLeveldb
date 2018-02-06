
void EncodeFixed32(char* buf, uint32_t value)
{
	if(port::kLittleEndian)
	{
		memcpy(buf, &value, sizeof(value));
	}
	else
	{
		buf[0]= value  &  0xff;
		buf[1]= (value  >>  8)  &  0xff;
		buf[2]= (value  >>   16)  &  0xff;
		buf[3]= (value  >>  24)  &  0xff;
	}
}

// 返回 v 有几个7位，能表示几个unsigned char字符
int VarintLength(uint64_t v)
{
	int len=1;
	while(v>=128) // v超过7位
	{
		v>>=7;
		len++;
	}
	return len;
}

// 解码 p到limit的字符串中的 第一个uint32_t返回给value
const char* GetVarint32PtrFallback(const char* p, const char* limit, uint32_t* value)
{
	uint32_t result=0;
	for(uint32_t shift=0; shift<=28  &&  p<limit; shift+=7)
	{
		uint32_t byte=*(reinterpret_cast<const unsigned char*>(p));
		p++;
		if(byte & 128) // byte第8位是1
		{
			result |= ((byte & 127)  << shift); // 保留byte的右7位
		}
		else
		{
			result |= (byte << shift);
			*value = result;
			return reinterpret_cast<const char*>(p);
		}
	}
	return NULL;
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value)
{
	uint64_t result=0;
	for(uint32_t shift=0; shift <= 63  &&  p<limit; shift += 7)
	{
		uint64_t byte=*(reinterpret_cast<const unsigned char*>(p));
		p++;
		if(byte  &  128) // byte第8位为1
			result |= ((byte & 127) << shift); // 保留byte的右7位
		else
		{
			result |= (byte << shift);
			*value = result;
			return reinterpret_cast<const char*>(p);
		}
	}
	return NULL;
}

// 编码v到dst中，返回dst前进到的位置
// 从右往左 每次取v的右边7位，写到dst
// ascii字符 0-127
// 相当于 将 按字节8位 写到dst 处理成 按7位写到dst, 第8位添加1
// 例外：最后一个7位串 的第8位保持为0的原因：隔离多个被编码到一个字符串中的uint32_t，便于解码
char* EncodeVarint32(char* dst, uint32_t v)
{
	// unsigned char能表示的整数范围 0-255
	unsigned char* ptr=reinterpret_cast<unsigned char*>(dst);
	static const int B=128; // (24位0) 1000 0000; 借助B保留v的右7位，第8位对于unsigned char可以忽略
	if( v < (1<<7) )
		*(ptr++)=v; // v最大 0111 1111
	else if( v < (1<<14))
	{
		*(ptr++)= v | B; // 将v的最右边7位取出,在左边添加一个1,8位写到ptr,左边剩余位数写到了ptr+1,被下一句覆盖
		*(ptr++)= v >> 7; // 写第二个7位，例外：第8位不为1
	}
	else if( v < (1<<21))
	{
		*(ptr++)= v | B;
		*(ptr++)= (v>>7) | B;
		*(ptr++)= v>>14; // 例外：最后一个7位串 的第8位不修改为1
	}
	else if( v < (1<<28))
	{
		*(ptr++)=v | B;
		*(ptr++)= (v >> 7) | B;
		*(ptr++)= (v >> 14) | B;
		*(ptr++)= v >> 21;
	}
	else // uint32_t 最多5个7位
	{
		*(ptr++)= v | B;
		*(ptr++)= (v>> 7) | B;
		*(ptr++)= (v >> 14) | B;
		*(ptr++)= (v >> 21) | B;
		*(ptr++)= v >> 28;
	}
	return reinterpret_cast<char*>(ptr);
}

char* EncodeVarint64(char* dst, uint64_t v)
{
	static const int B=128;
	unsigned char* ptr=reinterpret_cast<unsigned char*>(dst):
	while(v >= B) // 
	{
		*(ptr++)= (v & (B-1)) | B; // v  &  (0111 1111)  |  1000 0000, 取出右7位，添第8位1
		v >>= 7;
	}
	*(ptr++)=static_cast<unsigned char>(v); // v<B,  最后一个7位串，低8位保持不变,第8位仍为0
	return  reinterpret_cast<char*>(ptr);
}

void PutVarint32(std::string* dst, uint32_t v)
{
	char buf[5];
	char* ptr=EncodeVarint32(buf, v);
	dst->append(buf, ptr-buf);
}

void PutVarint64(std::string* dst, uint64_t v)
{
	char buf[10];
	char* ptr=EncodeVarint64(buf, v);
	dst->append(buf, ptr-buf);
}

void PutLengthPrefixedSlice(std::string* dst, const Slice& value)
{
	PutVarint32(dst, value.size());
	dst->append(value.data(), value.size());
}

bool GetVarint32(Slice* input, uint32_t* value)
{
	const char* p=input->data();
	const char* limit= p + input->size();
	const char* q=GetVarint32Ptr(p, limit, value);
	if(q==NULL)
		return false;
	else
	{
		*input=Slice(q, limit-q);
		return true;
	}
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result)
{
	uint32_t len;
	if(GetVarint32(input, &len)  &&  input->size() >= len)
	{
		*result = Slice(input->data(), len);
		input->remove_prefix(len);
		return true;
	}
	else
		return false;
}