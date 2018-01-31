
namespace leveldb{


Options SantizeOptions(const string& dbname, const InternalKeyComparator* icmp, const InternalFilterPolicy* ipolicy, const Options& src)
{
	
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr)
{
	*dbptr=NULL;
	
	DBImpl* impl=new DBImpl(options, dbname); // dbname是数据库的完整主目录
	impl->mutex_.Lock(); // MutexLock
	
	VersionEdit edit;
	
	bool save_manifest=false;
	Status s=impl->Recover(&edit, &save_manifest);
}


DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
	:env_(raw_options.env),
	internal_comparator_(raw_options.comparator),
	

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest)
{
	mutex_.AssertHeld(); // 已加锁
	
	env_->CreateDir(dbname_);
	assert(db_lock_==NULL);
	Status s=env_->LockFile(LockFileName(dbname_), &db_lock_); // 加文件/目录锁
	if(!s.ok())
		return s;
	
	if(!env_->FileExists(CurrentFileName(dbname_))) // 当前数据库目录下的CURRENT文件完整路径名
	{
		if(options_.create_if_missing)
		{
			
		}
	}
}

}