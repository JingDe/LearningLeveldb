
struct TableBuilder::Rep{
	Options options;
	Options index_block_options;
	WritableFile* file;
	uint64_t offset; // 跟踪当前写到file中的位置
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
	BlockHandle pending_handle; // 添加到index块的handle，表示数据块的位置和长度
	
	std::string compressed_output;
	
	Rep(const Options& opt, WritableFile* f)
		:options(opt),
		index_block_options(opt),
		offset(0),
		data_block(&options),
		index_block(&index_block_options),
		filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
	
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

// 写一个data block，计算filter block数据
void TableBuilder::Flush()
{
	Rep* r=rep_;
	assert(!r->closed);
	if(!ok())
		return;
	if(r->data_block.empty())
		return;
	assert(!r->pending_index_entry);
	WriteBlock(&r->data_block, &r->pending_handle); // 写一个数据块到文件中，更新r->pending_handle
	if(ok())
	{
		r->pending_index_entry=true;
		r->status = r->file->Flush();
	}
	if(r->filter_block != NULL)
		r->filter_block->StartBlock(r->offset);
}

// table 文件包含一系列block，每个block的格式：
// block_data: uint8[n]
// type: uint8
// crc: uint32
// 从BlockBuilder获得数据，调用WriteRawBlock写进文件
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
			break;
		case kSnappyCompression:
			std::string* compressed=&r->compressed_output;
			if(port::Snappy_Compress(raw.data(), raw.size(), compressed)  &&  compressed->size() < raw.size() - (raw.size() / 8u))
			{
				block_contents=*compressed;
			}
			else
			{
				block_contents=raw;
				type=kNoCompression;
			}
			break;
	}
	WriteRawBlock(block_contents, type, handle);
	r->compressed_output.clear();
	block->Reset();
}

// 写block内容，更新r->offset，返回handle表示当前写的内容的位置和长度
void TableBuilder::WriteRawBlock(const Slice& block_contents, CompressionType type, BlockHandle* handle)
{
	Rep* r=rep_;
	handle->set_offset(r->offset);
	handle->set_size(block_contents.size());
	r->status=r->file->Append(block_contents);
	if(r->status.ok())
	{
		char trailer[kBlockTrailerSize];
		trailer[0]=type;
		uint32_t crc=crc32c::Value(block_contents.data(), block_contents.size());
		crc=crc32c::Extend(crc, trailer, 1);
		EncodeFixed32(trailer+1, crc32c::Mask(crc));
		r->status=r->file->Append(Slice(trailer, kBlockTrailerSize));
		if(r->status.ok())
			r->offset += block_contents.size() + kBlockTrailerSize;
	}
}

Status TableBuilder::status() const {
	return rep_->status;
}


Status TableBuilder::Finish()
{
	Rep* r=rep_;
	Flush(); // 写最后一个数据块
	assert(!r->closed);
	r->closed=true;
	
	BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;
	
	if(ok()  &&  r->filter_block != NULL) // 写filter block
		WriteRawBlock(r->filter_block->Finish(), kNoCompression, &filter_block_handle); 
	
	if(ok())
	{
		BlockBuilder meta_index_block(&r->options);
		if(r->filter_block != NULL)
		{
			std::string key="filter.";
			key.append(r->options.filter_policy->Name());
			std::string handle_encoding;
			filter_block_handle.EncodeTo(&handle_encoding);
			meta_index_block.Add(key, handle_encoding); // 记录filter块的信息：名字--位置和长度
		}
		// TODO(postrelease): Add stats and other meta blocks
		WriteBlock(&meta_index_block, &metaindex_block_handle); // 写metaindex块
	}
	
	if(ok())
	{
		if(r->pending_index_entry)
		{
			r->options.comparator->FindShortestSeparator(&r->last_key);
			std::string handle_encoding;
			r->pending_handle.EncodeTo(&handle_encoding);// r->pending_handle指向最后一个数据块
			r->index_block.Add(r->last_key, Slice(handle_encoding)); // 记录数据块的信息：last key--最后一个数据块位置和长度
			r->pending_index_entry=false;
		}
		WriteBlock(&r->index_block, &index_block_handle); // 写index块
	}
	
	if(ok())
	{
		Footer footer;
		footer.set_metaindex_handle(metaindex_block_handle);
		std::string footer_encoding;
		footer.EncodeTo(&footer_encoding);
		r->status=r->file->Append(footer_encoding);
		if(r->status.ok())
			r->offset += footer_encoding.size();
	}
	return r->status;
}

Status TableBuilder::status() const {
	return rep_->status;
}

uint64_t TableBuilder::FileSize() const {	
	return rep_->offset;
}