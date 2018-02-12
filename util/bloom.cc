

static uint32_t BloomHash(const Slice& key) {
	return Hash(key.data(), key.size(), 0xbc9f1d34);
}

class BloomFilterPolicy: public FilterPolicy{
	
	virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const
	{
		size_t bits= n* bits_per_key_;
		if(bits<64)
			bits=64;
		
		size_t bytes=(bits+7)/8;
		bits=bytes*8;
		
		const const size_t init_size=dst->size();
		dst->resize(init_size + bytes, 0);
		dst->push_back(static_cast<char>(k_));
		char* array = &(*dst)[init_size];
		for(int i=0; i<n; i++)
		{
			uint32_t h=BloomHash(keys[i]);
			const uint32_t delta=(h>>17)  |  (h<<15); // 将h的高15位与低17位对调位置??
			for(size_t j=0; j<k_; j++)
			{
				const uint32_t bitpos=h % bits;
				array[bitpos/8] |= (1 << (bitpos % 8));
				h += delta;
			}
		}
	}
};