
class WriteBatch{
	
	class Handler{
	public:
		virtual void Put(const Slice& key, const Slice& value)=0;
		virtual void Delete(const Slice& key)=0;
	};

private:
	friend class WriteBatchInternal;
	
	std::string rep_;
};