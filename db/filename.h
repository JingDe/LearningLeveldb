
namespace leveldb{

enum FileType{
	kLogFile,
	kDBLockFile,
	kTableFile,
	kDescriptorFile,
	kCurrentFile,
	kTempFile,
	kInfoLogFile
};

extern std::string CurrentFileName(const std::string& dbname);

extern std::string InfoLogFileName(const std::string& dbname);

extern std::string OldInfoLogFileName(const std::string& dbname);
}