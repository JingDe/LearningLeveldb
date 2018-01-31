
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

}