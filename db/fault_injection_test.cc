
Status Truncate(const std::string& filename, uint64_t length)
{
	leveldb::Env* env=leveldb::Env::Default();
	
	SequentialFile* orig_file;
	Status s=env->NewSequentialFile(filename, &orig_file);
	if(!s.ok())
		return s;
	
	char* scratch=new char[length];
	leveldb::Slice result;
	s=orig_file->Read(length, &result, scratch);
	delete orig_file;
	if(s.ok())
	{
		std::string tmp_name=GetDirName(filename) + "/truncate.tmp";
		WritableFile* tmp_file;
		s=env->NewWritableFile(tmp_name, &tmp_file);
		if(s.ok())
		{
			s=tmp_file->Append(result);
			delete tmp_file;
			if(s.ok())
				s=env->RenameFile(tmp_name, filename);
			else
				env->DeleteFile(tmp_name);
		}
	}
}

struct FileState{
	std::string filename_;
	ssize_t pos_;
	ssize_t pos_at_last_sync_;
	ssize_t pos_at_last_flush_;
	
	bool IsFullySynced() const { return pos_ <= 0 || pos_ == pos_at_last_sync_; }

	Status DropUnsyncedData() const;
};

Status FileState::DropUnsyncedData() const
{
	ssize_t sync_pos=pos_at_last_sync_ == -1  ?  0  : pos_at_last_sync_;
	return Truncate(filename_, sync_pos);
}



class FaultInjectionTestEnv : public EnvWrapper {
	
	void ResetState();
	void SetFilesystemActive(bool active) { filesystem_active_ = active; }
	
	
private:
  port::Mutex mutex_;
  std::map<std::string, FileState> db_file_state_; // 数据库所有文件
  std::set<std::string> new_files_since_last_dir_sync_;
  bool filesystem_active_;  // Record flushes, syncs, writes
};

void FaultInjectionTestEnv::ResetState() {
	// 
  MutexLock l(&mutex_);
  SetFilesystemActive(true);
  
  Status DeleteFilesCreatedAfterLastDirSync();
}

Status FaultInjectionTestEnv::DropUnsyncedFileData()
{
	Status s;
	MutexLock l(&mutex_);
	for(std::map<std::string, FileState>::const_iterator it=db_file_state_.begin(); 
			s.ok()  &&  it!=db_file_state_.end(); ++it)
	{
		const FileState& state=it->second;
		if(!state.IsFullySynced())
			s=state.DropUnsyncedFileData();
	}
}

Status FaultInjectionTestEnv::DeleteFilesCreatedAfterLastDirSync()
{
	mutex_.Lock();
	std::set<std::string> new_files(new_files_since_last_dir_sync_.begin(), new_files_since_last_dir_sync_.end());
	mutex_.Unlock();
	
	Status s;
	std::set<std::string>::const_iterator it;
	for(it =new_files.begin(); s.ok()  &&  it!=new_files.end(); ++it)
		s=DeleteFile(*it);
	return s;
}

void FaultInjectionTestEnv::UntrackFile(const std::string& f) {
  MutexLock l(&mutex_);
  db_file_state_.erase(f);
  new_files_since_last_dir_sync_.erase(f);
}

Status FaultInjectionTestEnv::DeleteFile(const std::string& f) {
  Status s = EnvWrapper::DeleteFile(f);
  ASSERT_OK(s);
  if (s.ok()) {
    UntrackFile(f);
  }
  return s;
}



class FaultInjectionTest{
	
	enum ExpectedVerifResult { VAL_EXPECT_NO_ERROR, VAL_EXPECT_ERROR };
	enum ResetMethod { RESET_DROP_UNSYNCED_DATA, RESET_DELETE_UNSYNCED_FILES };
	
	Status OpenDB()
	{
		env_->ResetState();
		return 
	}
	
	void DoTest()
	{
		Random rnd(0);
		ASSERT_OK(OpenDB());
		for(size_t idx=0; idx < kNumIterations; idx++)
		{
			int num_pre_sync = rnd.Uniform(kMaxNumValues);
			int num_post_sync = rnd.Uniform(kMaxNumValues);
	  
			PartialCompactTestPreFault(num_pre_sync, num_post_sync);
			PartialCompactTestReopenWithFault(RESET_DROP_UNSYNCED_DATA,
                                        num_pre_sync,
                                        num_post_sync);
			NoWriteTestPreFault();
			NoWriteTestReopenWithFault(RESET_DROP_UNSYNCED_DATA);
		}
	}
	
	void PartialCompactTestPreFault(int num_pre_sync, int num_post_sync)
	{
		DeleteAllData();
		Build(0, num_pre_sync);
		db_->CompactRange(NULL, NULL);
		Build(num_pre_sync, num_post_sync);
	}
	
	void PartialCompactTestReopenWithFault(ResetMethod reset_method, int num_pre_sync, int num_post_sync)
	{
		env_->SetFilesystemActive(false);
		CloseDB();
		ResetDBState(rest_method);
		
		ASSERT_OK(OpenDB());
		ASSERT_OK(Verify(0, num_pre_sync, FaultInjectionTestEnv::VAL_EXPECT_NO_ERROR));
		ASSERT_OK(Verify(num_pre_sync, num_post_sync, FaultInjectionTestEnv::VAL_EXPECT_ERROR));
	}
	
	void Build(int start_idx, int num_vals)
	{
		std::string key_space, value_space;
		WriteBatch batch;
		for(int i=start_idx, i<start_idx+num_vals; i++)
		{
			Slice key=Key(i, &key_space);
			batch.Clear();
			batch.Put(key, Value(i, &value_space));
			ASSERT_OK(db_->Write(options, &batch));
		}
	}
	
	void DeleteAllData() {
		Iterator* iter = db_->NewIterator(ReadOptions());
		WriteOptions options;
		for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		  ASSERT_OK(db_->Delete(WriteOptions(), iter->key()));
		}

		delete iter;
	}
	
	void ResetDBState(ResetMethod reset_method)
	{
		switch(reset_method)
		{
		case RESET_DROP_UNSYNCED_DATA:
			ASSERT_OK(env_->DropUnsyncedFileData());
			break;
		  case RESET_DELETE_UNSYNCED_FILES:
			ASSERT_OK(env_->DeleteFilesCreatedAfterLastDirSync());
			break;
		  default:
			assert(false);
		}
	}
	
	Status ReadValue(int i, std::string* val) const
	{
		std::string key_space, value_space;
		Slice key=Key(i, &key_space);
		Value(i, &value_space);
		ReadOptions options;
		return db_->Get(options, key, val);
	}
	
	Status Verify(int start_idx, int num_vals, ExpectedVerifResult expected) const
	{
		std::string val;
		std::string value_space;
		for(int i=start_idx; i<start_idx+num_vals  &&  s.ok(); i++)
		{
			Value(i, &value_space);
			s=ReadValue(i, &val);
			if(expected == VAL_EXPECT_NO_ERROR)
			{
				if(s.ok())
					ASSERT_EQ(value_space, val);
			}
			else if(s.ok())
			{
				fprintf(stderr, "Expected an error at %d, but was OK\n", i);
				s = Status::IOError(dbname_, "Expected value error:");
			}
			else
				
		}
		return s;
	}
};