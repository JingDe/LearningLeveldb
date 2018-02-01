
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

}