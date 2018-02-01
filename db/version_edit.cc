
namespace leveldb{

enum Tag{
	kComparator = 1,
};

void VersionEdit::Clear()
{
	comparator_.clear();
	log_number_ = 0;
	prev_log_number_ = 0;
	last_sequence_ = 0;
	next_file_number_ = 0;
	has_comparator_ = false;
	has_log_number_ = false;
	has_prev_log_number_ = false;
	has_next_file_number_ = false;
	has_last_sequence_ = false;
	deleted_files_.clear();
	new_files_.clear();
}

void VersionEdit::Encode(std::string* dst) const
{
	if(has_comparator_)
	{
		PutVarint32(dst, kComparator);
		
	}
}

}