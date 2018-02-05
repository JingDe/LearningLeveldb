
namespace leveldb{

const int kNumNonTableCacheFiles = 10;


// 以下 修整用户提供的options 合理
template<class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue)
{
	if(static_cast<V>(*ptr) > maxvalue)
		*ptr=maxvalue;
	if(static_cast<V>(*ptr) < minvalue)
		*ptr=minvalue;
}

Options SantizeOptions(const string& dbname, const InternalKeyComparator* icmp, const InternalFilterPolicy* ipolicy, const Options& src)
{
	Options result=src;
	result.comparator=icmp;
	result.filter_policy=(src.filter_policy !=NULL) ? ipolicy  :  NULL;
	ClipToRange(&result.max_open_files, 64+kNumNonTableCacheFiles, 50000);
	ClipToRange(&result.write_buffer_size, 64<<10, 1<<30);
	ClipToRange(&result.max_file_size, 1<<20, 1<<30);
	ClipToRange(&result.block_size,        1<<10,                       4<<20);
	if(result.info_log ==NULL)
	{
		src.env->CreateDir(dbname);
		src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
		Status s=src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
		if(!s.ok())
			result.info_log=NULL;
	}
	if(result.block_cache==NULL)
		result.block_cache=NewLRUCache(8<<20);
	return result;
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
	internal_filter_policy_(raw_options.filter_policy_),
	options_(SantizeOptions(dbname, &internal_comparator_, &internal_filter_policy_, raw_options)),
	dbname_(dbname)
{
	
	versions_ = new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_);
}
	

Status DBImpl::NewDB()
{
	VersionEdit new_db;
	new_db.SetComparatorName(user_comparator()->Name());
	new_db.SetLogNumber(0);
	new_db.SetNextFile(2);
	new_db.SetLastSequence(0);
	
	const std::string manifest=DescriptorFileName(dbname_, 1); // dbname_/MANIFEST-000001
	WritableFile* file;
	Status s=env_->NewWritableFile(manifest, &file);
	if(!s.ok())
		return s;
	
	{
		log::writer log(file);
		std::string record;
		new_db.EncodeTo(&record);
		s=log.AddRecord(record); // VersionEdit编码成字符串写进 dbname_/MANIFEST-000001
		if(s.ok())
			s=file->Close();
	}
	delete file;
	if(s.ok())
		s=SetCurrentFile(env_, dbname_, 1); // 创建dbname_/CURRENT 写有 dbname_/MANIFEST-000001
	else
		env_->DeleteFile(manifest);
	return s;
}
	
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
			s=NewDB();
			if(!s.ok())
				return s;
		}
		else
		{
			return Status::InvalidArgument(dbname_, "does not nexit (create_if_missing is false)");
		}
	}
	else
	{
		if(options_.error_if_exits)
		{
			return Status::InvalidArgument(dbname_, "exits (error_if_exits is true)");
		}
	}
	
	s=versions_->Recover(save_manifest); // 根据当前数据库文件恢复
}

}