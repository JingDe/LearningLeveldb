
struct TableBuilder::Rep{
	Options options;
	Options index_block_options;
	WritableFile* file;
	uint64_t offset;
	Status status;
	BlockBuilder data_block;
	BlockBuilder index_block;
	std::string last_key;
	int64_t num_entries;
	bool closed;          // 调用Finish()或Abandon()
	FilterBlockBuilder* filter_block;
	
	// 不暴露一个block的index条目，直到看到下一个数据block的第一个key
	// 允许我们在index条目使用更短的key
	// For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
	bool pending_index_entry;// 只有当data_block为空时, r->pending_index_entry为true
	BlockHandle pending_handle; // 添加到index块的handle
	
	std::string compressed_output;
	
	Rep(const Options& opt, WritableFile* f)
		:options(opt),
		index_block_options(opt),
		
		data_block(&options),
		index_block(&index_block_options),
		
	
};

// SSTble中添加一个key-value，分别在索引块和数据块添加
void TableBuilder::Add(const Slice& key, const Slice& value)
{
	Rep* r=rep_;
	assert(!r->close);
	if(!ok())
		return true;
	if(r->num_entries >0)
		assert(r->options.comparator->Compare(key, Slice(r->last_key)) >0);
	
	if(r->pending_index_entry)
	{
		assert(r->data_block.empty());
		r->options.comparator->FindShortestSeparator(&r->last_key, key); // 找 r->last_key和key之间的最短key，赋给r->last_key
		std::string handle_encoding;
		r->pending_handle.EncodeTo(&handle_encoding);
		r->index_block.Add(r->last_key, Slice(handle_encoding)); // 索引块
		r->pending_index_entry=false;
	}		
	
	if(r->filter_block != NULL)
		r->filter_block->AddKey(key);
	
	r->last_key.assign(key.data(), key.size());
	r->num_entries++;
	r->data_block.Add(key, value); // 数据块
	
	const size_t estimated_block_size=r->data_block.CurrentSizeEstimate(); // 数据块大小
	if(estimated_block_size >= r->options.block_size)
		Flush();
}

void TableBuilder::Flush()
{
	Rep* r=rep_;
	assert(!r->closed);
	if(!ok())
		return;
	if(r->data_block.empty())
		return;
	assert(!r->pending_index_entry);
	WriteBlock(&r->data_block, &r->pending_handle);
	
}

// table 文件包含一系列block，每个block的格式：
// block_data: uint8[n]
// type: uint8
// crc: uint32
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle)
{
	assert(ok());
	Rep* r=rep_;
	Slice raw=block->Finish();
	
	Slice block_contents;
	CompressionType type=r->options.compression;
	switch(type)
	{
		case kNoCompression:
			block_contents=raw;
	}
}

Status TableBuilder::status() const {
	return rep_->status;
}