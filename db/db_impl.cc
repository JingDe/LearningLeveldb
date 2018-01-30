
Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr)
{
	*dbptr=NULL;
	
	DBImpl* impl=new DBImpl(options, dbname);
	impl->mutex_.Lock(); // MutexLock
	
	VersionEdit edit;
	
	bool save_manifest=false;
	Status s=impl->Recover(&edit, &save_manifest);
}

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest)
{
	mutex_.AssertHeld(); // 已加锁
	
	env_->CreateDir(dbname_);
	assert(db_lock_==NULL);
	Status s=env_->LockFile(LockFileName(dbname_), &db_lock_); // 加文件锁
	
}