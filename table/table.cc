
struct Table::Rep{
	Options options; // block_cache成员表示是否有block缓存
	Status status;
	RandomAccessFile* file;
	uint64_t cache_id; // 表示block缓存信息
	FilterBlockReader* filter;
	const char* filter_data; // filter块的数据

	BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
	Block* index_block;
	
	~Rep()
	{
		delete filter;
		delete [] filter_data;
		delete index_block;
	}
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
	
	Iterator* iter=meta->NewIterator(BytewiseComparator()); // metaindex块的迭代器
	std::string key="filter.";
	key.append(rep_->options.filter_policy->Name());
	iter->Seek(key); // metaindex块中查找指定filter的handle
	if(iter->Valid()  &&  iter->key()==Slice(key))
		ReadFilter(iter->value()); // 根据handle读取filter块
	delete iter;
	delete meta;
}

// 根据filter的handle的字符串描述读取filter块
void Table::ReadFilter(const Slice& filter_handle_value)
{
	Slice v=filter_handle_value;
	BlockHandle filter_handle;
	if(!filter_handle.DecodeFrom(&v).ok())
		return ;
	
	ReadOptions opt;
	if(rep_->options.paranoid_checks)
		opt.verify_checksums=true;
	BlockContents block;
	if(!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) // 读取filter块的数据
		return ;
	if(block.heap_allocated)
		rep_->filter_data = block.data.data();
	rep_->filter=new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
	delete rep_;
}

// 转换一个index迭代器值到相应block的内容上的迭代器
// index_value是index块的一个值，表示一个数据块的位置。先读取block数据
Iterator* Table::BlockReader(void *arg, const ReadOptions& options, const Slice& index_value)
{
	Table* table=reinterpret_cast<Table*>(arg);
	Cache* block_cache = table->rep_->options.block_cache;
	Block* block=NULL;
	Cache::Handle* cache_handle=NULL;
	
	BlockHandle handle;
	Slice input=index_value;
	Status s=handle.DecodeFrom(&input);
	
	if(s.ok())
	{
		BlockContents contents;
		if(block_cache !=NULL) // 从block缓存中读取block数据
		{
			char cache_key_buffer[16];
			EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
			EncodeFixed64(cache_key_buffer+8, handle.offset());
			Slice key(cache_key_buffer, sizeof(cache_key_buffer)); // cache_id+offset
			cache_handle = block_cache->Lookup(key);
			if(cache_handle !=NULL)
				block=reinterpret_cast<Block*>(block_cache->Value(cache_handle));
			else // block缓存未读到
			{
				s=ReadBlock(table->rep_->file, options, handle, &contents);
				if(s.ok())
				{
					block=new Block(contents);
					if(contents.cachable  &&  options.fill_cache)
					{
						cache_handle = block_cache->Insert(key, block, block->size(), &DeleteCachedBlock);
					}
				}
			}
		}
		else
		{
			s=ReadBlock(table->rep_->file, options, handle, &contents); // 直接从文件读取block数据
			if(s.ok())
				block=new Block(contents);
		}
	}
	
	Iterator* iter;
	if(block != NULL)
	{
		iter=block->NewIterator(table->rep_->options.comparator);
		// if(cache_handle == NULL)
			// iter->RegisterCleanup(&DeleteBlock, block, NULL);
		// else
			// iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
	}
	else 
	{
		iter = NewErrorIterator(s);
	}
	return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const
{
	return NewTwoLevelIterator(
		rep_->index_block->NewIterator(rep_->options.comparator), // 索引块的迭代器
		&Table::BlockReader, // 获得数据block的迭代器的函数
		const_cast<Table*>(this),
		options
	);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg, 
		void (*saver)(void*, const Slice&, const Slice&))
{
	Status s;
	Iterator* iiter =rep_->index_block->NewIterator(rep_->options.comparator);
	iiter->Seek(k);
	if(iiter->Valid())
	{
		Slice handle_value=iiter->value();
		FilterBlockReader* filter=rep_->filter;
		BlockHandle handle;
		if(filter!=NULL  &&  handle.DecodeFrom(&handle_value).ok()  &&  !filter->KeyMayMatch(handle.offset(), k))
		{}
		else
		{
			Iterator* block_iter =BlockReader(this, options, iiter->value());
			block_iter->Seek(k);
			if(block_iter->Valid())
				(*saver)(arg, block_iter->key(), block_iter->value());
			s=block_iter->status();
			delete block_iter;
		}
	}
	
}

static void DeleteCachedBlock(const Slice& key, void* value)
{
	Block* block=reinterpret_cast<Block*>(value);
	delete block;
}