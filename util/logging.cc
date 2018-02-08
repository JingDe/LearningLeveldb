
bool ConsumeDecimalNumber(Slice* in, uint64_t* val)
{
	uint64_t v=0;
	int digits=0;
	while(!in->empty())
	{
		char c=(*in)[0];
		if(c >= '0'  &&  c<='9')
		{
			++digits;
			const uint64_t delta=(c-'0');
			static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
			if(v > kMaxUint64/10  ||  (v==kMaxUint64/10  &&  delta > kMaxUint64 % 10))
			{
				return false;
			}
			v = (v*10) + delta;
			in->remove_prefix(1);
		}
		else
			break;
	}
	*val=v;
	return (digits>0);
}