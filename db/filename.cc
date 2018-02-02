
namespace leveldb{

std::string CurrentFileName(const std::string& dbname)
{
	return dbname+"/CURRENT";
}

std::string InfoLogFileName(const std::string& dbname)
{
	return dbname+"/LOG";
}

std::string OldInfoLogFileName(const std::string& dbname)
{
	return dbname+"/LOG.old";
}

std::string DescriptorFileName(const std::string& dbname, uint64_t number)
{
	assert(number>0);
	char buf[100];
	snprintf(buf, sizeof(buf), "/MANIFEST-%06llu", static_cast<unsigned long long>(number));
	return dbname+buf;
}

static std::string MakeFileName(const std::string& name, uint64_t number, const char* suffix)
{
	char buf[100];
	snprintf(buf, sizeof(buf), "/%06llu.%s", static_cast<unsigned long long>(number), suffix);
}

std::string TempFileName(const std::sttring& dbname, uint64_t number)
{
	assert(number > 0);
	return MakeFileName(dbname, number, "dbtmp");
}

// SetCurrentFile(env_, dbname_, 1)
Status SetCurrentFile(Env* env, const std::string& dbname, uint64_t descriptor_number)
{
	std::string manifest=DescriptorFileName(dbname, descriptor_number); // db_name/MANIFEST-%06llu
	Slice contents=manifest;
	assert(contents.starts_with(dbname+"/"));
	contents.remove_prefix(dbname.size()+1);
	std::string tmp=TempFileName(dbname, descriptor_number);
	Status s=
}

}