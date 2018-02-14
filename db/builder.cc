

// 从*iter的内容创建一个Table文件，生成的文件将根据meta->number被命名。
// 成功，meta将存储生成table的元数据
Status BuildTable(const std::string& dbname,
                         Env* env,
                         const Options& options,
                         TableCache* table_cache,
                         Iterator* iter, // memtable的迭代器
                         FileMetaData* meta){
	Status s;
	meta->file_size=0;
	iter->SeekToFirst();
	
	std::string fname=TableFileName(dbname, meta->number); // .ldb
	if(iter->Valid())
	{
		WritableFile* file;
		s=env->NewWritableFile(fname, &file);
		if(!s.ok())
			return s;
		
		TableBuilder* builder=new TableBuilder(options, file);
		meta->smallest.DecodeFrom(iter->key());
		for(; iter->Valid(); iter->Next())
		{
			Slice key=iter->key();
			meta->largest.DecodeFrom(key);
			builder->Add(key, iter->value());
		}
		
		s=builder->Finish();
		if(s.ok())
		{
			meta->file_size = builder->FileSize();
			assert(meta->file_size > 0);
		}
		delete builder;
		
		if(s.ok())
			s=file->Sync();
		if(s.ok())
			s=file->close();
		delete file;
		file=NULL;
		
		if(s.ok())
		{
			// 验证文件可用
			Iterator* it=table_cache->NewIterator(ReadOptions(), meta->number, meta->file_size);
			s=it->status();
			delete it;
		}
	}
	
	if(!iter->status().ok())
		s=iter->status();
	
	if(s.ok()  &&  meta->file_size>0)
	{}
	else
		env->DeleteFile(fname);
	return s;
}