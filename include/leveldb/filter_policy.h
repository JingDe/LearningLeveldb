
class FilterPolicy{
	
	// keys[0, n-1]包含了一系列key（可以有重复），根据用户提供的comparator排序
	// 在*dst末尾添加一个summarize keys[0, n-1]的filter
	virtual void CreateFilter(const Slice* keys, int n, std::string* dst)
      const = 0;
};