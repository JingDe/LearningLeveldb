
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle)
{
	Status s;
	char buf[sizeof(file_number)];
	EncodeFixed64(buf, file_number);
	Slice key(buf, sizeof(buf));
	*handle=cache_->Lookup(key); // 根据文件序号在cache_中查找
	if(*handle == NULL)
	{
		std::string fname=TableFileName(dbname_, file_number); // .ldb文件
		RandomAccesssFile* file=NULL;
		Table* table=NULL;
		s=env_->NewRandomAccessFile(fname, &file);
		if(!s.ok())
		{
			std::string old_fname=SSTTableFileName(dbname_, file_number); // .sst文件
			if(env_->NewRandomAccessFile(old_fname, &file).ok())
				s=Status::OK();
		}
		if(s.ok())
			s=Table::Open(*options_, file, file_size, &table);
		
		
	}
}

Iterator* TableCache::NewIterator(const ReadOptions& options, 
					uint64_t file_number, uint64_t file_size, Table** tableptr)
{
	if(tableptr != NULL)
		*tableptr=NULL;
	
	Cache::Handle* handle=NULL;
	Status s=FindTable(file_number, file_size, &handle);
	
}