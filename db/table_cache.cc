
struct TableAndFile{
	RandomAccessFile* file;
	Table* table;
};

static void DeleteEntry(const Slice& key, void* value)
{
	TableAndFile* tf=reinterpret_cast<TableAndFile*>(value);
	delete tf->table; // 释放table的各个block数据
	delete tf->file; // 关闭文件，或者若是mmap实现的解除内存中的映射
}

// 从TableCache中查找指定序号的文件，获得其handle，若不存在时先进行插入
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
		RandomAccessFile* file=NULL;
		Table* table=NULL;
		s=env_->NewRandomAccessFile(fname, &file);
		if(!s.ok())
		{
			std::string old_fname=SSTTableFileName(dbname_, file_number); // .sst文件
			if(env_->NewRandomAccessFile(old_fname, &file).ok())
				s=Status::OK();
		}
		if(s.ok())
			s=Table::Open(*options_, file, file_size, &table); // 打开文件中的表
		
		if(!s.ok())
		{
			assert(table == NULL);
			delete file;
		}
		else
		{
			TableAndFile* tf = new TableAndFile;
			tf->file=file;
			tf->table=table;
			*handle=cache_->Insert(key, tf, 1, &DeleteEntry); // 缓存文件序号代表的文件指针和表
		}
	}
	return s;
}

// 从TableCache中查找指定序号的文件/表，创建表通过tableptr返回，并返回表上的迭代器
Iterator* TableCache::NewIterator(const ReadOptions& options, 
					uint64_t file_number, uint64_t file_size, Table** tableptr)
{
	if(tableptr != NULL)
		*tableptr=NULL;
	
	Cache::Handle* handle=NULL;
	Status s=FindTable(file_number, file_size, &handle); // 查找文件
	if(!s.ok())
	{
		return NewErrorIterator(s);
	}
	
	Table* table =reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
	Iterator* result=table->NewIterator(options); // 
	result->RegisterCleanup(&UnrefEntry, cache_, handle);
	if(tableptr != NULL)
		*tableptr=table;
	return result;
}