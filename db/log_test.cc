

class LogTest{
	
	class StringDest : public WritableFile
	{
		std::string contents_;
	};
	
	class StringSource: public SequentialFile
	{
		Slice contents_;
		bool force_error_;
		bool returned_partial_;
	};
	
	StringDest dest_;
	StringSource source_;
	bool reading_;
	
	
public:
    LogTest() : reading_(false),
              writer_(new Writer(&dest_)),
              reader_(new Reader(&source_, &report_, true/*checksum*/,
                      0/*initial_offset*/)) {
	}
	
	std::string Read()
	{
		if(!reading_)
		{
			reading_ =true;
			source_.contents_ = Slice(dest_.contents_);
		}
		std::string scratch;
		Slice record;
		if(reader_ ->ReadRecord(&record, &scratch))
			return record.ToString();
		else
			return "EOF";
	}
	
	void Write(const std::string& msg) {
		ASSERT_TRUE(!reading_) << "Write() after starting to read";
		writer_->AddRecord(Slice(msg));
	}
};

TEST(LogTest, Empty) {
  ASSERT_EQ("EOF", Read());
}

