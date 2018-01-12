
#ifnef SLICE_H
#define SLICE_H

#include<string>


class Slice{
public:
	//Slice():data_(NULL),size_(0){}
	Slice():data_(""),size_(0){} //为什么是""
	Slice(const char* d, size_t n):data_(d),size_(n){}
	Slice(const char* d):data_(d),size_(strlen(d)){}
	Slice(const std::string &s):data_(s.data()),size_(s.size()){}
	
	const char* data() const
	{
		return data_;
	}
	
	size_t size() const
	{
		return size_;
	}
	
	bool empty() const
	{
		return size_==0;
	}
	
	char operator[](size_t i)
	{
		assert(i < size());
		return data_[i];
		//return *(data_+i);
	}
	
	void clear()
	{
		data_="";
		size_=0;
	}
	
	std::string ToString() const
	{
		return std::string(data_, size_);
	}
	
	int compare(const Slice& other);
	
	bool starts_with(const Slice& buf) const;

private:
	const char* data_;
	size_t size_;
};

inline int Slice::compare(const Slice& b) const
{
	const size_t sz=(size_ < b.size_) ? size_ : b.size_;
	int ret=memcmp(data_, b.data(), sz);
	if(ret<0  ||  (ret==0  &&  size_<b.size())
		return -1;
	else if(ret>0  ||  (ret==0  &&  size_>b.size())
		return 1;
	return 0;
}

inline bool Slice::starts_with(const Slice& b) const
{
	return ( (size_>=b.size())  &&  (memcmp(data_, b.data(), b.size())==0) );
}


inline bool operator==(const Slice& s1, const Slice& s2)//inline
{
	//return ( (s1.size()==s2.size())  &&  (memcmp(s1.data(), s2.data(), x.size())==0) );
	return s1.compare(s2)==0  ?  true  :  false;
}

inline bool operator!=(const Slice& s1, const Slice& s2)//inline
{
	return !(s1==s2);
}

#endif