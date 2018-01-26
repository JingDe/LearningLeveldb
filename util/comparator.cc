
#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb{

Comparator::~Comparator() {}

namespace{
class BytewiseComparatorImpl: public Comparator{
public:
	BytewiseComparatorImpl(){}
	
	virtual const char* Name() const{
		return "leveldb.BytewiseComparator";
	}
	
	virtual int Compare(const Slice& a, const Slice& b) const {
		return a.compare(b);
	}
	
	// change *start to a short string in [start,limit).
	// 
	virtual void FindShortestSeparator(std::string* start, const Slice& limit) const
	{
		size_t min_length=std::min(start->size(), limit.size());
		size_t diff_index=0;
		while( (diff_index < min_length)  &&  ( (*start)[diff_index] == limit[diff_index] ))
		{
			diff_index++;
		}
		
		if(diff_index >= min_length) // 相同长度内，start等于limit时,start不变
		{
			
		}
		else
		{
			uint8_t diff_byte= static_cast<uint8_t>((*start)[diff_index]);
			if( diff_byte < static_cast<uint8_t>(0xff)  &&  diff_byte+1 < static_cast<uint8_t>(limit[diff_index]) )
			{ // start的第一个不同于limit的字符，若加1仍小于limit，将start该位加1，并截断其后的子串
				(*start)[diff_index]++;
				start->resize(diff_index+1);
				assert(Compare(*start, limit)<0);
			}
			// 当start 第一个不同位加1后大于等于limit相应位，start不变
		}
	}
	
	// 将key第一个可以增加1的字节增加1，并截断后面
	virtual void FindShortSuccessor(std::string* key) const
	{
		size_t n=key->size();
		for(size_t i=0; i<n; i++)
		{
			const uint8_t byte=(*key)[i];
			if(byte != static_cast<uint8_t>(0xff))
			{
				(*key)[i]=byte+1;
				key->resize(i+1);
				return ;
			}
		}
		// key是全部的1  0xff
	}
};
	
}


static port::OnceType once=LEVELDB_ONCE_INIT;
static const Comparator* bytewise;

static void InitModule()
{
	bytewise=new BytewiseComparatorImpl();
}

const Comparator* BytewiseComparator()
{
	port::InitOnce(&once, InitModule);
	return bytewise;
}

}