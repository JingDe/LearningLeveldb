
namespace leveldb{

std::string CurrentFileName(const std::string& dbname)
{
	return dbname+"/CURRENT";
}
	
}