
namespace leveldb{

// A utility routine: write "data" to the named file and Sync() it.
extern Status WriteStringToFileSync(Env* env, const Slice& data,
                                    const std::string& fname);
									
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
	return name+buf; // name/number.suffix
}

std::string TempFileName(const std::sttring& dbname, uint64_t number)
{
	assert(number > 0);
	return MakeFileName(dbname, number, "dbtmp"); // dbname/number.dbtmp
}

// SetCurrentFile(env_, dbname_, 1)
Status SetCurrentFile(Env* env, const std::string& dbname, uint64_t descriptor_number)
{
	std::string manifest=DescriptorFileName(dbname, descriptor_number); // db_name/MANIFEST-%06llu
	Slice contents=manifest;
	assert(contents.starts_with(dbname+"/"));
	contents.remove_prefix(dbname.size()+1); // MANIFEST-%06llu
	std::string tmp=TempFileName(dbname, descriptor_number); // dbname/descriptor_number.dbtemp
	Status s= WriteStringToFileSync(env, contents.ToString()+"\n", tmp); // 将MAFNIFEST文件名写到dbtemp文件中
	if(s.ok())
		s=env->RenameFile(tmp, CurrentFileName(dbname)); // 将dbtemp文件重命名为 db/CURRENT
	if(!s.ok())
		env->DeleteFile(tmp);
	return s;
}

bool ParseFileName(const std::string& fname, uint64_t* number, FileType* type)
{
	Slice rest(fname);
	if(rest == "CURRENT")
	{
		*number=0;
		*type=kCurrentFile;
	}
	else if(rest == "LOCK")
	{
		*number=0;
		*type=kDBLockFile;
	}
	else if(rest=="LOG"  ||  reset=="LOG.old")
	{
		*number=0;
		*type=kInfoLogFile;
	}
	else if(rest.starts_with("MANIFEST-"))
	{
		rest.remove_prefix(strlen("MANIFEST-"));
		uint64_t num;
		if(!ConsumeDecimalNumber(&rest, &num))
			return false;
		if(!rest.empty())
			return false;
		*type=kDescriptorFile;
		*number=num;
	}
	else
	{
		uint64_t num;
		if(!ConsumeDecimalNumber(&rest, &num))
			return false;
		Slice suffix=rest;
		if(suffix==Slice(".log"))
			*type=kLogFile;
		else if(suffix==Slice(".sst")  ||  suffix==Slice(".ldb"))
		{
			*type = kTableFile;
		}
		else if(suffix==Slice(".dbtmp"))
			*type=kTempFile;
		else
			return false;
		*number=num;
	}
	return true;
}

std::string TableFileName(const std::string& name, uint64_t number)
{
	assert(number>0);
	return MakeFileName(name, number, "ldb");
}

std::string SSTTableFileName(const std::string& name, uint64_t number) 
{
	assert(number > 0);
	return MakeFileName(name, number, "sst");
}

}