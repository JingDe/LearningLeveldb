
Status BuildTable(const std::string& dbname,
                         Env* env,
                         const Options& options,
                         TableCache* table_cache,
                         Iterator* iter,
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
	}
}