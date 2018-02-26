
namespace leveldb{

const int kNumNonTableCacheFiles = 10;


struct DBImpl::CompactionState{
	Compaction* const compaction;
	
	// 不需要服务小于 smallest_snapshot 的快照
	// 所以如果遇到小于等于smallest_snapshot的sequence number S，
	// 可以丢弃sequence number小于S的所有相同key的entry
	SequenceNumber smallest_snapshot;
	
	// compaction产生的文件
	struct Output {
		uint64_t number;
		uint64_t file_size;
		InternalKey smallest, largest;
	};
	std::vector<Output> outputs;
	
	// 正被生成的output的状态
	WritableFile* outfile;
	TableBuilder* builder;
	
	explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
    }
};


// 以下 修整用户提供的options 合理
template<class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue)
{
	if(static_cast<V>(*ptr) > maxvalue)
		*ptr=maxvalue;
	if(static_cast<V>(*ptr) < minvalue)
		*ptr=minvalue;
}

Options SanitizeOptions(const string& dbname, const InternalKeyComparator* icmp, const InternalFilterPolicy* ipolicy, const Options& src)
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
		Status s=src.env->NewLogger(InfoLogFileName(dbname), &result.info_log); // dbname/LOG文件
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
	if(s.ok()  &&  impl->mem_==NULL)
	{
		uint64_t new_log_number=impl->versions_->NewFileNumber();
		WritableFile* lfile; // 新log文件
		s=options.env->NewWritableFile(LogFileName(dbname, new_log_number), &lfile);
		if(s.ok())
		{
			edit.SetLogNumber(new_log_number);
			impl->logfile_ =lfile;
			impl->logfile_number_ =new_log_number;
			impl->log_ =new log::Writer(lfile);
			impl->mem_ =new MemTable(impl->internal_comparator_);
			impl->mem_->Ref();
		}
	}
	if(s.ok()  &&  save_manifest)
	{
		edit.SetPrevLogNumber(0);
		edit.SetLogNumber(impl->logfile_number_);
		s=impl->versions_->LogAndApply(&edit, &impl->mutex_);
	}
	if(s.ok())
	{
		impl->DeleteObsoleteFiles();
		impl->MaybeScheduleCompaction();
	}
}

void DBImpl::DeleteObsoleteFiles()
{
	if(!bg_error_.ok()) // 有后台错误后，不能确定是否一个新的version被提交，所以不能安全地收集垃圾
		return;
	
	// 所有live文件集合
	std::set<uint64_t> live=pending_outputs_;
	versions_->AddLiveFiles(&live); // 添加versions_ 的所有文件到live中
	
	std::vector<std::string> filenames;
	env_->GetChildren(dbname_, &filenames);
	uint64_t number;
	FileType type;
	for(size_t i=0; i<filenames.size(); i++)
	{
		if(ParseFileName(filenames[i], &number, &type))
		{
			bool keep=true;
			switch(type)
			{
				case kLogFile:
					keep = ((number >= versions_->LogNumber())  ||  (number == versions_->PrevLogNumber()));
					break;
				case kDescriptorFile:
					keep = (number >= versions_->ManifestFileNumber());
					break;
				case kTableFile:
					keep = (live.find(number) != live.end());
					break;
				case kTempFile:
					keep = (live.find(number) != live.end());
					break;
				case kCurrentFile:
				case kDBLockFile;
				case kInfoLogFile:
					keep =true;
					break;
			}
			
			if(!keep)
			{
				if(type == kTableFile)
					table_cache_->Evict(number);
				Log(options_.info_log, "Delete type=%d #%lld\n", int(type),
						static_cast<unsigned long long>(number));
				env_->DeleteFile(dbname_ +"/" + filenames[i]);
			}
		}
	}
}

void DBImpl::MaybeScheduleCompaction()
{
	mutex_.AssertHeld();
	if(bg_compaction_scheduled_)
	{}
	else if(shutting_down_.Acquire_Load())
	{}
	else if(!bg_error_.ok())
	{}
	else if(imm_ == NULL  &&  manual_compaction_ == NULL  &&  !versions_->NeedsCompaction())
	{}
	else
	{
		bg_compaction_scheduled_ =true;
		env_->Schedule(&DBImpl::BGWork, this);
	}
}

void DBImpl::BGWork(void *db)
{
	reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall()
{
	MutexLock l(&mutex_);
	assert(bg_compaction_scheduled_);
	if(shutting_down_.Acquire_Load())
	{}
	else if(!bg_error_.ok())
	{}
	else
		BackgroundCompaction();
	
	bg_compaction_scheduled_=false;
	MaybeScheduleCompaction();
	bg_cv_.SingnalAll();
}

void DBImpl::BackgroundCompaction()
{
	mutex_.AssertHeld();
	
	if(imm_ != NULL)
	{
		CompactMemTable();
		return;
	}
	
	Compaction* c;
	bool is_manual = (manual_compaction_ != NULL);
	InternalKey manual_end;
	if(is_manual)
	{
		ManualCompaction* m=manual_compaction_;
		c=versions_->CompactRange(m->level, m->begin, m->end);
		m->done = (c==NULL);
		if(c != NULL)
			manual_end = c->input(0, c->num_input_files(0)-1) ->largest; // 获得最大的InternalKey
		Log(options_.info_log,
			"Manual compaction at level-%d from %s .. %s; will stop at %s\n",
			m->level,
			(m->begin ? m->begin->DebugString().c_str() : "(begin)"),
			(m->end ? m->end->DebugString().c_str() : "(end)"),
			(m->done ? "(end)" : manual_end.DebugString().c_str()));
	}
	else
		c =versions_->PickCompaction();
	
	Status status;
	if(c ==NULL)
	{}
	else if(!is_manual  &&  c->IsTrivialMove()) // level+1层，没有与level层overlap
	{
		// 将文件移动到下一层
		assert(c->num_input_files(0) ==1);
		FileMetaData* f=c->input(0, 0);
		c->edit()->DeleteFile(c->level(), f->number);
		c->edit()->AddFile(c->level()+1, f->number, f->file_size, f->smallest, f->largest);
		status=versions_->LogAndApply(c->edit(), &mutex_);
		if(!status.ok())
			RecordBackgroundError(status);
		VersionSet::LevelSummaryStorage tmp;
		Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
			static_cast<unsigned long long>(f->number),
			c->level() + 1,
			static_cast<unsigned long long>(f->file_size),
			status.ToString().c_str(),
			versions_->LevelSummary(&tmp));
	}
	else
	{
		CompactionState* compact=new CompactionState(c);
		status =DoCompactionWork(compact);
		if(!status.ok())
			RecordBackgroundError(status);
		CleanupCompaction(compact);
		c->ReleaseInputs();
		DeleteObsoleteFiles();
	}
	delete c;
	
	
}

Status DBImpl::DoCompactionWork(CompactionState* compact)
{
	const uint64_t start_micros =env_->NowMicros();
	int64_t imm_micros=0;
	
	Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);
	  
	assert(versions_->NumLevelFiles(compact->compaction->level()) >0);
	assert(compact->builder ==NULL);
	assert(compact->outfile ==NULL);
	if(snapshots_.empty())
		compact->smallest_snapshot =versions_->LastSequence();
	else
		compact->smallest_snapshot =snapshots_.oldest()->number;
	
	mutex_.Unlock();
	
	Iterator* input=versions_->MakeInputIterator(compact->compaction); // 获得需要compaction的所有文件的迭代器
	input->SeekToFirst();
	Status status;
	ParsedInternalKey ikey;
	std::string current_user_key;
	bool has_current_user_key =false;
	SequenceNumber last_sequence_for_key =kMaxSequenceNumber;
	for(; input->Valid()  &&  !shutting_down_.Acquire_Load(); )
	{
		if(has_imm_.NoBarrier_Load() !=NULL)
		{
			const uint64_t imm_start=env_->NowMicros();
			mutex_.Lock();
			if(imm_ !=NULL)
			{
				CompactMemTable();
				bg_cv_.SignalAll();
			}
			mutex_.Unlock();
			imm_micros += (env_->NowMicros() -imm_start);
		}
		
		Slice key=input->key();
		if(compact->compaction->ShouldStopBefore(key)  &&  compact->builder !=NULL) 
			// 当前output的overlap太多，开始新的output
		{
			status=FinishCompactionOutputFile(compact, input);
			if(!status.ok())
				break;
		}
		
		// 处理key/value，添加到state
		bool drop=false;
		if(!ParseInternalKey(key, &ikey))
		{
			current_user_key.clear();
			has_current_user_key =false;
			last_sequence_for_key =kMaxSequenceNumber;
		}
		else
		{
			if(!has_current_user_key  ||  user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=0)
			{
				// 当前user key的首次出现
				current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
				has_current_user_key =true;
				last_sequence_for_key =kMaxSequenceNumber;
			}
			
			if(last_sequence_for_key <= compact->smallest_snapshot)
				drop=true; // 被一个拥有相同user key的更新的entry隐藏
			else if(ikey.type == kTypeDeletion  &&  ikey.sequence <= compact->smallest_snapshot  &&
					compact->compaction->IsBaseLevelForKey(ikey.user_key))
			{
				drop =true;
			}
			
			last_sequence_for_key =ikey.sequence;
		}
		
		if(!drop)
		{
			// 必要时打开output文件
			if(compact->builder ==NULL)
			{
				status =OpenCompactionOutputFile(compact);
				if(!status.ok())
					break;
			}
			if(compact->builder->NumEntries() ==0)
				compact->current_output()->smallest.DecodeFrom(key);
			compact->current_output()->largest.DecodeFrom(key);
			compact->builder->Add(key, input->value());
			
			
		}
		
		input->Next();
	}
	
	
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact)
{
	assert(compact !=NULL);
	assert(compact->builder ==NULL);
	uint64_t file_number;
	{
		mutex_.Lock();
		file_number =versions_->NewFileNumber();
		pending_outputs_.insert(file_number);
		CompactionState::Output out;
		
	}
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact, Iterator* input)
{
	assert(compact != NULL);
	assert(compact->outfile != NULL);
	assert(compact->builder != NULL);
	
	const uint64_t output_number =compact->current_output()->number;
	assert(output_number !=0);
	
	Status s=input->status();
	const uint64_t current_entries =compact->builder->NumEntries();
	if(s.ok())
		s=compact->builder->Finish();
	else
		compact->builder->Abandon();
	
	const uint64_t current_bytes =compact->builder->FileSize();
	compact->current_output()->file_size =current_bytes;
	compact->total_bytes += current_bytes;
	delete compact->builder;
	compact->builder =NULL;
	
	if(s.ok())
		s=compact->outfile->Sync();
	if(s.ok())
		s=compact->outfile->Close();
	delete compact->outfile;
	compact->outfile =NULL;
	
	if(s.ok()  &&  current_entries>0)
	{
		// 验证表可用
		Iterator* iter=table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
		s=iter->status();
		delete iter;
		if(s.ok())
		{
			Log(options_.info_log,
			  "Generated table #%llu@%d: %lld keys, %lld bytes",
			  (unsigned long long) output_number,
			  compact->compaction->level(),
			  (unsigned long long) current_entries,
			  (unsigned long long) current_bytes);
		}
	}
	return s;
}

void DBImpl::CleanupCompaction(CompactionState* compact)
{
	mutex_.AssertHeld();
	if(compact->builder !=NULL)
	{
		compact->builder->Abandon();
		delete compact->builder;
	}
	else
		assert(compact->outfile == NULL);
	
	delete compact->outfile;
	for(size_t i=0; i<compact->outputs.size(); i++)
	{
		const CompactionState::Output& out=compact->outputs[i];
		pending_outputs_.erase(out.number);
	}
	delete compact;
}

void DBImpl::CompactMemTable()
{
	mutex_.AssertHeld();
	assert(imm_ != NULL);
	
	VersionEdit edit;
	Version* base=versions_->current();
	base->Ref();
	Status s=WriteLevel0Table(imm_, &edit, base); // 写imm_到ldb文件
	base->Unref();
	
	if(s.ok()  &&  shutting_down_.Acquire_Load())
		s=Status::IOError("Deleting DB during memtable compaction");
	
	// 用生成的Table替换immutable memtable
	if(s.ok())
	{
		edit.SetPrevLogNumber(0);
		edit.SetLogNumber(logfile_number_); // 不需要以前的logs
		s=versions_->LogAndApply(&edit, &mutex_);
	}
	
	if(s.ok())
	{
		imm_->Unref();
		imm_ =NULL;
		has_imm_.Release_Store(NULL);
		DeleteObsoleteFiles();
	}
	else
		RecordBackgroundError(s);
}

void DBImpl::RecordBackgroundError(const Status& s) {
	mutex_.AssertHeld();
	if (bg_error_.ok()) {
		bg_error_ = s;
		bg_cv_.SignalAll();
	}
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
	:env_(raw_options.env),
	internal_comparator_(raw_options.comparator),
	internal_filter_policy_(raw_options.filter_policy_),
	options_(SanitizeOptions(dbname, &internal_comparator_, &internal_filter_policy_, raw_options)),
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
	
	s=versions_->Recover(save_manifest); // 根据当前数据库文件恢复，并判断是否需要保存当前的manifest
	if(!s.ok())
		return s;
	SequenceNumber max_sequence(0);
	
	// 恢复所有比descriptor即MANIFEST文件中新的log文件(.log文件)
	// 这些文件可能是先前的数据库化身创建的，但没有注册到descriptor中
	const uint64_t min_log=versions_->LogNumber();
	const uint64_t prev_log=versions_->PrevLogNumber();
	std::vector<std::string> filenames;
	s=env_->GetChildren(dbname_, &filenames); // 数据库目录下所有文件
	if(!s.ok())
		return s;
	std::set<uint64_t> expected;
	versions_->AddLiveFiles(&expected); // VersionSet 所有Version记录的文件 （的序号）
	uint64_t number;
	FileType type;
	std::vector<uint64_t> logs;
	for(size_t i=0; i< filenames.size(); i++)
	{
		if(ParseFileName(filenames[i], &number, &type))
		{
			expected.erase(number);
			if(type==kLogFile  &&  ((number >= min_log)  ||  (number==prev_log) ))
				logs.push_back(number); 
		}
	}
	if(!expected.empty()) // VersionSet 记录的文件中有丢失
	{
		char buf[50];
		snprintf(buf, sizeof(buf), "%d missing files; e.g.", static_cast<int>(expected.size()));
		return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin()))); // .ldb文件
	}
	
	std::sort(logs.begin(), logs.end()); // 所有 .log文件的序号
	for(size_t i=0; i<logs.size(); i++)
	{
		s=RecoverLogFile(logs[i], (i==logs.size()-1), save_manifest, edit, &max_sequence);
		if(!s.ok())
			return s;
		
		// 先前的数据库化身可能在分配log序号后没有写MANIFEST记录，在VersionSet中手动更新文件序号分配计数
		versions_->MarkFileNumberUsed(logs[i]);
	}
	
	if(versions_->LastSequence() < max_sequence)
		versions_->SetLastSequence(max_sequence);
	
	return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest, 
		VersionEdit* edit, SequenceNumber* max_sequence)
{
	struct LogReporter: public log::Reader::Reporter{
		Env* env;
		Logger* info_log;
		const char* fname;
		Status* status;
		virtual void Corruption(size_t bytes, const Status& s)
		{
			Log(info_log, "%s%s: dropping %d bytes; %s", (this->status==NULL  ?  "(ignoring error) " : ""),
					fname, static_cast<int>(bytes), s.ToString().c_str());
		}
	};
	
	mutex_.AssertHeld();
	
	std::string fname=LogFileName(dbname_, log_number); // .log文件
	SequentialFile* file;
	Status status=env_->NewSequentialFile(fname, &file);
	if(!status.ok())
	{
		MaybeIgnoreError(&status);
		return status;
	}
	
	LogReporter reporter;
	reporter.env=env_;
	reporter.info_log=options_.info_log;
	reporter.fname=fname.c_str();
	reporter.status=(options_.paranoid_checks ? &status : NULL);
	
	log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
	Log(options_.info_log, "Recovering log #%llu", (unsigned long long) log_number);
	
	std::string scratch;
	Slice record;
	WriteBatch batch;
	int compactions=0;
	MemTable* mem=NULL;
	while(reader.ReadRecord(&record, &scratch)  &&  status.ok())
	{
		if(record.size()<12)
		{
			reporter.Corruption(record.size(), Status::Corruption("log record too small"));
			continue;
		}
		WriteBatchInternal::SetContents(&batch, record);
		
		if(mem==NULL)
		{
			mem=new MemTable(internal_comparator_);
			mem->Ref();
		}
		status=WriteBatchInternal::InsertInto(&batch, mem); // 从记录中恢复MemTable
		MaybeIgnoreError(&status);
		if(!status.ok())
			break;
		// writebatch头有8字节sequence号+4字节count
		const SequenceNumber last_seq=WriteBatchInternal::Sequence(&batch) + WriteBatchInternal::Count(&batch) -1;
		if(last_seq > *max_sequence)
			*max_sequence = last_seq;
		
		// 当0层的memtable较大时，需要保存当前的manifest，将0层数据写进sstable
		if(mem->ApproximateMemoryUsage() > options_.write_buffer_size)
		{
			compactions++;
			*save_manifest=true;
			status=WriteLevel0Table(mem, edit, NULL);
			mem->Unref();
			mem=NULL;
			if(!stats.ok())
				break;
		}
	}
	
	delete file;
	
	// 检查是否可以重用最后的log文件
	if(status.ok()  &&  options_.reuse_logs  &&  last_log  &&  compactions==0)
	{
		assert(logfile_ == NULL);
		assert(log_ == NULL);
		assert(mem_ == NULL);
		uint64_t lfile_size;
		if(env_->GetFileSize(fname, &lfile_size).ok()  &&  env_->NewAppendableFile(fname, &logfile_).ok())
		{
			Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
			log_ =new log::Writer(logfile_, lfile_size);
			logfile_number_=log_number;
			if(mem !=NULL)
			{
				mem_=mem;
				mem=NULL;
			}
			else
			{
				mem_=new MemTable(internal_comparator_);
				mem_->Ref();
			}
		}
	}
	
	if(mem !=NULL)
	{
		if(status.ok())
		{
			*save_manifest=true;
			status=WriteLevel0Table(mem, edit, NULL);
		}
		mem->Unref();
	}
}

void DBImpl::MaybeIgnoreError(Status* s) const
{
	if(s.ok()  ||  options_.paranoid_checks)
	{
		
	}
	else
	{
		Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
		*s=Status::OK();
	}
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
{
	mutex_.AssertHeld();
	const uint64_t start_micros=env_->NowMicros();
	FileMetaData meta;
	meta.number=versions_->NewFileNumber(); 
	pending_outputs_.insert(meta.number);
	Iterator* iter=mem->NewIterator();
	Log(options_.info_log, "Level-0 table #%llu: started", (unsigned long long) meta.number);
	
	Status s;
	{
		mutex_.Unlock();
		s=BuildTable(dbname_, env_, options_, table_cache_, iter, &meta); // 从mem/iter中创建ldb文件,返回meta
		mutex_.Lock();
	}
	
	Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
			(unsigned long long) meta.number, (unsigned long long) meta.file_size,
			s.ToString().c_str());
	delete iter;
	pending_outputs_.erase(meta.number);
	
	int level=0;
	if(s.ok()  &&  meta.file_size >0)
	{
		const Slice min_user_key = meta.smallest.user_key();
		const Slice max_user_key = meta.largest.user_key();
		if(base != NULL)
			level=base->PickLevelForMemTableOutput(min_user_key, max_user_key);
		edit->AddFile(level, meta.number, meta.file_size, meta.smallest, meta.largest);
	}
	
	CompactionStats stats;
	stats.micros = env_->NowMicros() - start_micros;
	stats.bytes_written=meta.file_size;
	stats_[level].Add(stats);
	return s;
}

}