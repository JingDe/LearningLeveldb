
class EnvTest{
private:
	port::Mutex mu_;
	std::string events_;
};

TEST(EnvTest, ReadWrite)
{
	Random rnd(test::RandomSeed());
	
	std::string test_dir;
	ASSERT_OK(env_->GetTestDirectory(&test_dir));
	std::string test_file_name=test_dir + "/open_on_read.txt";
	WritableFile* writable_file;
	ASSERT_OK(env_->NewWritableFile(test_file_name, &writable_file));
	
	static const size_t kDataSize = 10 * 1048576;
	std::string data;
	while(data.size() < kDataSize)
	{
		int len=rnd.Skewed(18);
		std::string r;
		test::RandomString(&rnd, len, &r);
		ASSERT_OK(writable_file->Append(r));
		data += r;
		if(rnd.OneIn(10))
			ASSERT_OK(writable_file->Flush());
	}
	ASSERT_OK(writable_file->Sync());
	ASSERT_OK(writable_file->Close());
	delete writable_file;
	
	SequentialFile* sequential_file;
	ASSERT_OK(env_->NewSequentialFile(test_file_name, &sequential_file));
	std::
	while(read_result.size() < data.size())
	{
		int len=std::min<int>(rnd.Skewed(18), data.size()-read_result.size());
		scratch.resize(std::max(len, 1));
		Slice read;
		ASSERT_OK(sequential_file->Read(len, &read, &scratch[0]);
		if(len > 0)
			ASSERT_GET(read.size(), 0);
		ASSERT_LE(read.size(), len);
		read_result.append(read.data(), read.size());
	}
	ASSERT_EQ(read_result, data);
	delete 
}