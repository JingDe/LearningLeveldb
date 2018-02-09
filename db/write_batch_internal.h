
class WriteBatchInternal{
public:
	static void SetContents(WriteBatch* batch, const Slice& contents);
	
	static Status InsertInto(const WriteBatch* batch, MemTable* memtable);
	
	static SequenceNumber Sequence(const WriteBatch* batch);
};