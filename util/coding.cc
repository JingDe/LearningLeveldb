
// 编码v到dst中，返回dst前进到的位置
char* EncodeVarint32(char* dst, uint32_t v)
{
	unsigned char* ptr=reinterpret_cast<unsigned char*>(dst);
	static const int B=128;
	if( v < (1<<7) )
		*(ptr++)=v;
	else if( v < (1<<14))
	{
		*(ptr++)= v | B;
		*(ptr++)= v >> 7;
	}
	else if( v < (1<<21))
	{
		*(ptr++)= v | B;
		*(ptr++)= (v>>7) | B;
		*(ptr++)= v>>14;
	}
}

void PutVarint32(std::string* dst, uint32_t v)
{
	char buf[5];
	char* ptr=EncodeVarint32(buf, v);
	dst->append(buf, ptr-buf);
}