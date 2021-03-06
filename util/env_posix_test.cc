static const int kDelayMicros = 100000;
static const int kReadOnlyFileLimit = 4;
static const int kMMapLimit = 4;

class EnvPosixTest{
public:
	Env* env_;
	EnvPosixTest() : env_(Env::Default()) {}
	
	static void SetFileLimits(int read_only_file_limit, int mmap_limit)
	{
		EnvPosixTestHelper::SetReadOnlyFDLimit(read_only_file_limit);
		EnvPosixTestHelper::SetReadOnlyMMapLimit(mmap_limit);
	}
};

TEST(EnvPosixTest, TestOpenOnRead)
{
	std::string test_dir;
	ASSERT_OK(env_->GetTestDirectory(&test_dir));
	std::string test_file=test_dir+"/open_on_read.txt";
	
	FILE* f=fopen(test_file.c_str(), "w");
	ASSERT_TRUE(f !=NULL);
	const char kFileData[]="abcdefghijklmnopqrstuvwxyz";
	fputs(kFileData, f);
	fclose(f);
	
	const int kNumFiles=kReadOnlyFileLimit + kMMapLimit + 5;
	leveldb::RandomAccessFile* files[kNumFiles]={0};
	for(int i=0; i<kNumFiles; i++)
		ASSERT_OK(env_->NewRandomAccessFile(test_file, &files[i]));
	
	char scratch;
	Slice read_result;
	for(int i=0; i<kNumFiles; i++) // kNumFiles== 13
	{
		ASSERT_OK(files[i]->Read(i, 1, &read_result, &scratch)); // 读偏移量i处的一个字符到 read_result
		ASSERT_EQ(kFileData[i], read_result[0]); // 读到的字符等于 kFileData 的第i个字符
	}
	for(int i=0; i<kNumFiles; i++)
		delete files[i];
	ASSERT_OK(env_->DeleteFile(test_file));
}