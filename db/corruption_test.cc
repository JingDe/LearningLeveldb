
class CorruptionTest{
public:
	test::ErrorEnv env_;
	std::string dbname_;
	Cache* tiny_cache_;
	Options options_;
	DB* db_;
	
	CorruptionTest()
	{
		tiny_cache_ =NewLRUCache(100);
		options_.env = &env_;
		options_.block_cache = tiny_cache_;
		dbname_ =test::TmpDir() + "/corruption_test";
		DestroyDB(dbname_, options_);
		
		db_=NULL;
		options_.create_if_missing = true;
		Reopen();
		options_.create_if_missing = false;
	}
	
	Status TryReopen()
	{
		delete db_;
		db_=NULL;
		return DB::Open(options_, dbname_, &db_);
	}
	
	void Reopen() {
		ASSERT_OK(TryReopen());
	}
		
	void Build(int n)
	{
		std::string key_space, value_space;
		WriteBatch batch;
		for(int i=0; i<n; i++)
		{
			Slice key=Key(i, &key_space); // key是i
			batch.Clear();
			batch.Put(key, Value(i, &value_space));
			WriteOptions options;
			if(i == n-1)
				options.sync=true;
			ASSERT_OK(db_->Write(options, &batch));
		}
	}
	
	void Check(int min_expected, int max_expected)
	{
		int next_expected=0;
		int missed=0;
		
		Iterator* iter=db_->NewIterator(ReadOptions());
		for(iter->SeekToFirst(); iter->Valid(); iter->Next())
		{
			uint64_t key;
			Slice in(iter->key());
			if(in == ""  ||  in=="~")
			{
				continue;
			}
			if(!ConsumeDecimalNumber(&in, &key)  ||  !in.empty()  ||  key<next_expected)
			{
				bad_keys++;
				continue;
			}
			missed += (key-next_expected);
			next_expected=key+1;
			if(iter->value() != Value(key, &value_space))
				bad_values++;
			else
				correct++;
		}
		delete iter;
		
		fprintf(stderr,
            "expected=%d..%d; got=%d; bad_keys=%d; bad_values=%d; missed=%d\n",
            min_expected, max_expected, correct, bad_keys, bad_values, missed);
		ASSERT_LE(min_expected, correct);
		ASSERT_GE(max_expected, correct);
	}
	
	void Corrupt(FileType filetype, int offset, int bytes_to_corrupt)
	{
		std::vector<std::string> filenames;
		ASSERT_OK(env_.GetChildren(dbname_, &filenames));
		uint64_t number;
		
		for(size_t i=0; i<filenames.size(); i++)
		{
			if(ParseFileName(filenames[i], &number, &type)  &&  type==filetype  &&  int(number)>picked_number)
			{
				fname=dbname_+"/"+filenames[i];
				picked_number=number;
			}
		}
		ASSERT_TRUE(!fname.empty()) << filetype;
		
		struct stat sbuf;
		if(stat(fname.c_str(), &sbuf) !=0)
		{
			const char* msg = strerror(errno);
			ASSERT_TRUE(false) << fname << ": " << msg;
		}
		
		if(offset <0)
		{
			if(0-offset > sbuf.st_size)
				offset=0;
			else
				offset=sbuf.st_size + offset;
		}
		
		if(offset > subf.st_size)
			offset = sbuf.st_size;
		if(offset+bytes_to_corrupt > sbuf.st_size)
			bytes_to_corrupt = sbuf.st_size - offset;
		
		std::string contents;
		Status s=ReadFileToString(Env::Default(), fname, &contents);
		for(int i=0; i<bytes_to_corrupt; i++)
			contents[i+offset] ^=0x80;
		s=WriteStringToFile(Env::Default(), contents, fname);
		
	}
	
	Slice Key(int i, std::string* storage) {
		char buf[100];
		snprintf(buf, sizeof(buf), "%016d", i);
		storage->assign(buf, strlen(buf));
		return Slice(*storage);
	}
	  Slice Value(int k, std::string* storage) {
    Random r(k);
    return test::RandomString(&r, kValueSize, storage);
  }
};

TEST(CorruptionTest, Recovery) {
  Build(100);
  Check(100, 100);
  Corrupt(kLogFile, 19, 1);      // WriteBatch tag for first record
  Corrupt(kLogFile, log::kBlockSize + 1000, 1);  // Somewhere in second block
  Reopen();

  // The 64 records in the first two log blocks are completely lost.
  Check(36, 36);
}

