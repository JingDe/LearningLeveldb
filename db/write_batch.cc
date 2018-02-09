

// writebatch头有8字节sequence号+4字节count
static const size_t kHeader = 12;

WriteBatch::WriteBatch()
{
	Clear();
}

void WriteBatch::Clear()
{
	rep_.clear();
	rep_.resize(kHeader);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents)
{
	assert(contents.size() >= kHeader);
	b->rep_.assign(contents.data(), contents.size());
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b)
{
	return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

int WriteBatchInternal::Count(const WriteBatch* b) {
	return DecodeFixed32(b->rep_.data() + 8);
}

namespace{

class MemTableInserter: public WriteBatch::Handler{
public:
	SequenceNumber sequence_; // uint64_t 
	MemTable* mem_;
	
	virtual void Put(const Slice& key, const Slice& value)
	{
		mem_->Add(sequence_, kTypeValue, key, value);
		sequence_++;
	}
	virtual void Delete(const Slice& key)
	{
		mem_->Add(sequence_, kTypeDeletion, key, Slice());
		sequence_++;
	}
};

}

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable)
{
	MemTableInserter inserter;
	inserter.sequence_=WriteBatchInternal::Sequence(b);
	inserter.mem_=memtable;
	return b->Iterate(&inserter);
}

// 将 rep_ 插入到 handler.mem_中
Status WriteBatch::Iterate(Handler* handler) const
{
	Slice input(rep_);
	if(input.size() < kHeader)
		return Status::Corruption("malformed WriteBatch (too small)");
	
	input.remove_prefix(kHeader);
	Slice key, value;
	int found=0;
	while(!input.empty())
	{
		found++;
		char tag=input[0];
		input.remove_prefix(1);
		switch(tag)
		{
		case kTypeValue:
			if(GetLengthPrefixSlice(&input, &key)  &&  GetLengthPrefixSlice(&input, &value))
				handler->Put(key, value);
			else
				return Status::Corruption("bad WriteBatch Put");
			break;
		caes kTypeDeletion:
			if(GetLengthPrefixSlice(&input, &key))
				handler->Delete(key);
			
		}
	}
}