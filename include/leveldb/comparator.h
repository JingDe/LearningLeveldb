
#include <string>
#include "leveldb/export.h"

//Comparator类提供key的排序
//要求线程安全，leveldb可能在多个线程中并发调用其方法
class Comparator{
public:
	virtual ~Comparator();
	
	virtual int Compare(const Slice& a, const Slice& b) const=0;
	
	virtual const char* Name() const=0;
	
	virtual void FindShortestSeparator(std::string* start, const Slice& limit) const =0;
	
	virtual void FindShortSuccessor(std::string* key) const=0;
};

const Comparator* BytewiseComparator();
