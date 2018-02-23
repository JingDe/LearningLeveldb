
// 文件的引用计数、大小、数据块
class FileState{
	
	FileState() : refs_(0), size_(0) {}
	
	void Ref() {
		MutexLock lock(&refs_mutex_);
		++refs_;
	}
	
	Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
	{
		if(offset > size_)
			return Status::IOError("Offset greater than ");
		const uint64_t available =size_-offset;
		if(n > available)
			n=static_cast<size_t>(available);
		if(n==0)
		{
			*result=Slice();
			return Status::OK();
		}
		
		assert(offset / kBlockSize <= SIZE_MAX);
		size_t block=static_cast<size_t>(offset / kBlockSize);
		size_t block_offset= offset % kBlockSize;
		
		if(n <= kBlockSize - block_offset)
		{
			*result = Slice(blocks_[block] + block_offset, n);
			return Status::OK();
		}
		
		size_t bytes_to_copy =n;
		char* dst=scratch;
		while(bytes_to_copy > 0)
		{
			size_t avail=kBlockSize-block_offset;
			if(avail > bytes_to_copy)
				avail=bytes_to_copy;
			memcpy(dst, blocks_[block]+block_offset, avail);
			
			bytes_to_copy -=avail;
			dst += avail;
			block ++;
			block_offset =0;
		}
		
		*result=Slice(scratch, n);
		return Status::OK();
	}
	
	port::Mutex refs_mutex_;
	int refs_;
	
	std::vector<char*> blocks_; // 内存环境，文件在内存中
	uint64_t size_;
	
	enum { kBlockSize = 8 * 1024 };
};

// FileState、文件位置
class SequentialFileImpl: public SequentialFile{

	explicit SequentialFileImpl(FileState* file): file_(file), pos_(0){
		file_->Ref();
	}
	
	~SequentialFileImpl() {
		file_->Unref();
	}
	
private:
	FileState* file_;
	uint64_t pos_;
};

class InMemoryEnv: public EnvWrapper{
public:
	explicit InMemoryEnv(Env* base_env) : EnvWrapper(base_env) { }
	
	virtual Status NewSequentialFile(const std::string& fname, SequentialFile** result)
	{
		MutexLock lock(&mutex_);
		if(file_map_.find(name) == file_map_.end())
		{
			*result=NULL;
			return Status::IOError(fname, "File not found");
		}
		*result=new SequentialFileImpl(file_map_[fname]);
		return Status::OK();
	}
	
private:
	typedef std::map<std::string, FileState*> FileSystem;
	port::Mutex mutex_;
	FileSystem file_map_;
};

Env* NewMemEnv(Env* base_env) {
	return new InMemoryEnv(base_env);
}