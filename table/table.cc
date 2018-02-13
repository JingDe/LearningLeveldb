
struct Table::Rep{
	Options options;
	Status status;
	RandomAccessFile* file;
	uint64_t cache_id;
	FilterBlockReader* filter;
	const char* filter_data;

	BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
	Block* index_block;
};

// 打开存储在file中的表
Status Table::Open(const Options& options, RandomAccessFile* file, uint64_t size, Table** table)
{
	*table = NULL;
	if(size < Footer::kEncodedLength)
		return Status::Corruption("file is too short to be an sstable");
	
	// 读取footer
	char footer_space[Footer::kEncodedLength];
	Slice footer_input;
	Status s=file->Read(size-Footer::kEncodedLength, Footer::kEncodedLength, &footer_input, footer_space);
	if(!s.ok())
		return s;
	
	Footer footer;
	s=footer.DecodeFrom(&footer_input);
	if(!s.ok())
		return s;
	
	// 读取index block
	BlockContents index_block_contents;
	if(s.ok())
	{
		ReadOptions opt;
		if(options.paranoid_checks)
			opt.verify_checksums=true;
		s=ReadBlock(file, opt, footer.index_handle(), &index_block_contents);
	}
	
	if(s.ok())
	{
		Block* index_block=new Block(index_block_contents);
		Rep* rep=new Table::Rep;
		rep->options=options;
		rep->file=file;
		rep->metaindex_handle=footer.metaindex_handle();
		rep->index_block=index_block;
		rep->cache_id=(options.block_cache ? options.block_cache->NewId()  : 0);
		rep->filter_data =NULL;
		rep->filter=NULL;
		*table=new Table(rep);
		(*table)->ReadMeta(footer);
	}
	return s;
}

void Table::ReadMeta(const Footer& footer)
{
	if(rep_->options.filter_policy == NULL)
		return ;
	
	ReadOptions opt;
	if(rep_->options.paranoid_checks)
		opt.verify_checksums=true;
	BlockContents contents;
	if(!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok())
		return ;
	Block* meta=new Block(contents);
	
	Iterator* iter=meta->NewIterator(BytewiseComparator());
	std::string key="filter.";
	key.append(rep_->options.filter_policy->Name());
	iter->Seek(key);
	
}

