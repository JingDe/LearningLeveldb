
static int TargetFileSize(const Options* options) {
	return options->max_file_size;
}

static int64_t MaxGrandParentOverlapBytes(const Options* options) {
	return 10 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level)
{
	double result= 10. * 1048576.0;
	while(level > 1)
	{
		result *=10;
		level --;
	}
	return result;
}

static Iterator* GetFileIterator(void* arg, const ReadOptions& options, const Slice& file_value)
{
	TableCache* cache=reinterpret_cast<TableCache*>(arg);
	if(file_value.size() != 16)
		return NewErrorIterator();
	else
		return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
}


int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files, const Slice& key)
{
	uint32_t left=0;
	uint32_t right=files.size();
	while(left < right)
	{
		uint32_t mid=(left + right) /2;
		const FileMetaData* f=files[mid];
		if(icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) <0)
			left=mid+1;
	}
}

// 一个内部迭代器
// 对给定的version/level对，生成关于level层文件的信息
// 对给定的entry，key()是出现在文件中的最大key，
// value()是一个16位值，包含使用EncodeFixed64编码的文件序号和文件大小
class Version::LevelFileNumIterator: public Iterator{
	
	Slice key() const {
		assert(Valid());
		return (*flist_)[index_]->largest.Encode();
	}
	
	Slice value() const {
		assert(Valid());
		EncodeFixed64(value_buf_, (*flist_)[index_]->number);
		EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size);
		return Slice(value_buf_, sizeof(value_buf_));
	}
	
	const std::vector<FileMetaData*>* const flist_;
	uint32_t index_;
	
	// 
	mutable char value_buf_[16];
};

void Version::Unref()
{
	assert(this != &vset_->dummy_versions_);
	assert(refs_ >=1);
	--refs_;
	if(refs_ == 0)
		delete this;
}

void Version::OverlapInLevel(int level, const Slice* smallest_user_key, const Slice* largest_user_key)
{
	return SomeFileOverlapsRange(vset_->icmp_, (level>0), file_[level], smallest_user_key, largest_user_key);
}

// 在某一层的files中查找是否有重叠的key，对files重叠的0层遍历，files不重叠的其他层二分查找
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) 
{
	const Comparator* ucmp=icmp.user_comparator();
	if(!disjoint_sorted_files) // level==0, files重叠
	{
		for(size_t i=0; i<files.size(); i++)
		{
			const FileMetaData* f=files[i];
			if(AfterFile(ucmp, smallest_user_key, f)  ||  BeforeFile(ucmp, largest_user_key, f))
			{
				//不重叠
			}
			else
				return true;
		}
		return false;
	}
	
	uint32_t index=0;
	if(smallest_user_key != NULL)
	{
		InternalKey small(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
		index =FindFile(icmp, files, small.Encode());
	}
	
	if(index >= files.size())
		return false;
	
	return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// 返回应该在其中放置一个新的memtable compaction结果的层，
// 这个结果覆盖了[smallest_user_key,largest_user_key]
int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key, const Slice& largest_user_key)
{
	int level=0;
	if(!OverlapInLevel(0, &smallest_user_key, &largest_user_key))
	{
		// 如果下一层没有重叠，并且下下一层重叠的字节有限，push到下一层
		InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
		InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
		std::vector<FileMetaData*> overlaps;
		while(level < config::kMaxMemCompactLevel)
		{
			if(OverlapInLevel(level +1, &smallest_user_key, &largest_user_key))
				break;
			if(level+2 < config::kNumLevels) // 检查文件不与太多的祖父字节重叠
			{
				GetOverlappingInputs(level+2, &start, &limit, &overlaps);
				const int64_t sum=TotalFileSize(overlaps);
				if(sum > MaxGrandParentOverlapBytes(vset_->options_))
					break;
			}
			level++;
		}
	}
	return level;
}


VersionSet::VersionSet(const std::string& dbname, const Options* options, TableCache* table_cache, const InternalKeyComparator* cmp)
	: env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // 从Recover()获得
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this),
      current_(NULL) 
{
	AppendVersion(new Version(this));
}

void VersionSet::AppendVersion(Version* v)
{
	assert(v->refs_==0);
	assert(v != current_); // current_ == dummy_versions_.prev_
	if(current_ !=NULL)
		current_->Unref();
	current_=v;
	v->Ref();
	
	v->prev_=dummy_versions_.prev; // 将v插入到 dummy_versions_之前（current_位置）
	v->next_=dummy_versions_;
	v->perv_->next_ =v;
	v->next_->prev_ =v;
}

// 在inputs中存储level层所有与[begin, end]重叠的文件
void Version::GetOverlappingInputs(int level, const InternalKey* begin, const InternalKey* end, 
		std::vector<FileMetaData*>* inputs)
{
	assert(level >=0);
	assert(level < config::kNumLevels);
	inputs->clear();
	Slice user_begin, user_end;
	if(begin != NULL)
		user_begin = begin->user_key();
	if(end != NULL)
		user_end = end->user_key();
	
	const Comparator* user_cmp=vset_->icmp_.user_comparator();
	for(size_t i=0; i<files_[level].size(); )
	{
		FileMetaData* f=files_[level][i++];
		const Slice file_start=f->smallest.user_key();
		const Slice file_limit=f->largest.user_key();
		if(begin !=NULL  &&  user_cmp->Compare(file_limit, user_begin)<0)
		{}
		else if(end!=NULL  &&  user_cmp->Compare(file_start, user_end) > 0)
		{}
		else
		{
			inputs->push_back(f);
			if(level ==0)
			{
				// level 0层的文件可能相互重叠，检查加入的文件是否拓展了范围
				// 是，则重新检查level 0所有文件
				if(begin !=NULL  &&  user_cmp->Compare(file_start, user_begin) <0)
				{
					user_begin=file_start;
					inputs->clear();
					i=0;
				}
				else if(end != NULL  &&  user_cmp->Compare(file_limit, user_end) >0)
				{
					user_end=file_limit;
					inputs->clear();
					i=0;
				}
			}
		}
	}
}

class VersionSet::Builder{
	
	struct BySmallestKey{
		const InternalKeyComparator* internal_comparator;
		
		bool operator()(FileMetaData* f1, FileMetaData* f2) const // 比较文件最小key
		{
			int r=internal_comparator->Compare(f1->smallest, f2->smallest);
		}
	};
	
	typedef std::set<FileMetaData*, BySmallestKey> FileSet; // 文件集合，使用 BySmallestKey排序
	struct LevelState{ // 某一层的删除文件集合和有效文件集合
		std::set<uint64_t> deleted_files;
		FileSet* added_files;
	};

	VersionSet* vset_;
	Version* base_;
	LevelState levels_[config::kNumLevels]; //每一层的删除文件和有效文件
	
	
	
public:
	Builder(VersionSet* vset, Version* base)
		:vset_(vset),
		base_(base)
	{
		base_->Ref(); // 增加引用计数，不会释放
		BySmallestKey cmp;
		cmp.internal_comparator= &vset_->icmp_;
		for(int level=0; level<config::kNumLevels; level++)
			levels_[level].added_files= new FileSet(cmp);
	}
	
	void Apply(VersionEdit* edit) // 将edit应用到当前状态
	{
		// 更新compaction pointers，删除文件，加入文件
		for(size_t i=0; i<edit->compact_pointers_.size(); i++)
		{
			const int level=edit->compact_pointers_[i].first;
			vset_->compact_pointers_[level]=edit->compact_pointers_[i].second.Encode().ToString();
		}
		
		const VersionEdit::DeletedFileSet& del=edit->deleted_files_;
		for(VersionEdit::DeletedFileSet::const_iterator iter=del.begin(); iter!=del.end(); ++iter)
		{
			const int level=iter->first;
			const uint64_t number=iter->second;
			levels_[level].deleted_files.insert(number);
		}
		
		for(size_t i=0; i<edit->new_files_.size(); i++)
		{
			const int level=edit->new_files_[i].first;
			FileMetaData* f=new FileMetaData(edit->new_files_[i].second);
			f->refs=1;
			
			f->allowed_seeks=(f->file_size / 16384);
			if(f->allowed_seeks < 100)
				f->allowed_seeks=100;
			
			levels_[level].deleted_files.erase(f->number);
			levels_[level].added_files->insert(f);
		}
	}
	
	void SaveTo((Version* v)
	{
		BySmallestKey cmp;
		cmp.internal_comparator=&vset_->icmp_;
		for(int level=0; level<config::kNumLevels; level++)
		{
		// 将added files加入已存在的文件，插入过程中忽略删除的文件，将结果存到v中
			// 归并顺序插入两个文件数组
			const std::vector<FileMetaData*>& base_files=base_->files_[level];
			std::vector<FileMetaData*>::const_iterator base_iter=base_files.begin();
			std::vector<FileMetaData*>::const_iterator base_end=base_files.end();
			const FileSet* added=levels_[level].added_files;
			v->files_[level].reserve(base_files.size() + added->size());
			// 将 added 每一个FileSet* 添加到base_files中
			for(FileSet::const_iterator added_iter=added->size(); added_iter!=added->end(); ++added_iter)
			{
				// 将 base_files 中每一个小于 此次added元素 的文件先插入
				std::vector<FileMetaData*>::const_iterator bpos=std::upper_bound(base_iter, base_end, *added_iter, cmp);
				for(; base_iter != bpos; ++base_iter)
				{
					MaybeAddFile(v, level, *base_iter);
				}
				// 后插入added的一个文件
				MaybeAddFile(v, level, *added_iter);
			}
			
				// 插入base_files中剩下的文件
			for(; base_iter != base_end; ++base_iter)
				MaybeAddFile(v. level, *base_iter);
			
			// 
			if(level>0)
			{
				for(uint32_t i=1; i< v->files_[level].size(); i++)
				{
					const InternalKey& prev_end=v->files_[level][i-1]->largest;
					const InternalKey& this_begin=v->files_[level][i]->smallest;
					if(vset_->icmp_.Compare(prev_end, this_begin) >=0)
					{
						fprintf(stderr, "overlapping ranges in same level %s vs. %s\n", prev_end.DebugString().c_str(), this_begin.DebugString().c_str());
						abort();
					}
				}
			}
		}
	}
	
	// 在Version中插入第level层的文件f，从小到大插入，保证不overlap
	void MaybeAddFile(Version* v, int level, FileMetaData* f)
	{
		if(levels_[level].deleted_files.count(f->number) >0)
		{}
		else
		{
			std::vector<FileMetaData*>* files=&v->files_[level];
			if(level >0  &&  !files->empty())
				assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest, f->smallest) < 0);
			f->refs++;
			files->push_back(f);
		}
	}
};

Status VersionSet::Recover(bool *save_manifest)
{
	struct LogReporter: public log::Reader::Reporter{
		Status* status;
		virtual void Corruption(size_t bytes, const Status& s)
		{
			if (this->status->ok()) 
				*this->status = s;
		}
	};
	
	std::string current;
	Status s=ReadFileToString(env_, CurrentFileName(dbname_), &current); // dbname_/CURRENT 文件
	if(!s.ok())
		return s;
	if(current.empty()  ||  current[current.size()-1] != '\n')
		return Status::Corruption("CURRENT file does not end with newline");
	current.resize(current.size() -1);
	
	std::string dscname=dbname_ + "/" +current; // dbname_/MANIFEST-xxxxxx
	SequentialFile* file;
	s=env_->NewSequentialFile(dscname, &file);
	if(!s.ok())
	{
		if(s.IsNotFound())
			return Status::Corruption("CURRENT points to a non-existent file", s.ToString());
		return s;
	}
	
	bool have_log_number=false;
	bool have_prev_log_number = false;
	bool have_next_file = false;
	bool have_last_sequence = false;
	uint64_t next_file = 0;
	uint64_t last_sequence = 0;
	uint64_t log_number = 0;
	uint64_t prev_log_number = 0; 
	Builder builder(this, current_);
	
	{
		LogReporter reporter; // 将 s 传给 reader , ReadRecord 出错时通过 s 返回结果
		reporter.status=&s;
		log::Reader reader(file, &reporter, true, 0); // MANIFEST-xxxxxx文件
		Slice record;
		std::string scratch;
		while(reader.ReadRecord(&record, &scratch)  &&  s.ok()) // 通过 s 检查 ReadRecord是否成功
		{
			VersionEdit edit;
			s=edit.DecodeFrom(record);
			if(s.ok())
			{
				if(edit.has_comparator_  &&  edit.comparator_ !=icmp_.user_comparator()->Name())
				{
					s=Status::InvalidArgument(edit.comparator_ + "does not match existing comparator", icmp_.user_comparator()->Name());
				}
			}
			
			if(s.ok())
				builder.Apply(&edit);
			
			if(edit.has_log_number_)
			{
				log_number = edit.log_number_;
				have_log_number=true;
			}
			
			if (edit.has_prev_log_number_) {
				prev_log_number = edit.prev_log_number_;
				have_prev_log_number = true;
			}

			if (edit.has_next_file_number_) {
				next_file = edit.next_file_number_;
				have_next_file = true;
			}

			if (edit.has_last_sequence_) {
				last_sequence = edit.last_sequence_;
				have_last_sequence = true;
			}
		}
	}
	delete file;
	file=NULL;
	
	if(s.ok())
	{
		if(!have_next_file)
		{	s=Status::Corruption("no meta-nextfile entry in descriptor");
		} else if (!have_log_number) {
		  s = Status::Corruption("no meta-lognumber entry in descriptor");
		} else if (!have_last_sequence) {
		  s = Status::Corruption("no last-sequence-number entry in descriptor");
		}
		
		if(!have_prev_log_number)
			prev_log_number=0;
		
		MarkFileNumberUsed(prev_log_number);
		MarkFileNumberUsed(log_number);
	}
	if(s.ok())
	{
		Version* v=new Version(this);
		builder.SaveTo(v);
		
		// 安装新创建的version
		Finalize(v); 
		AppendVersion(v);
		manifest_file_number_=next_file;
		next_file_number_=next_file+1;
		last_sequence_=last_sequence;
		log_number_=log_number;
		prev_log_number_=prev_log_number;
		
		if(ReuseManifest(dscname, current))
		{
			// 可以使用存在的MANIFEST文件，不需要保存新的
		}
		else
			*save_manifest = true;
	}
	
	return s;
}

// 不能重用MANIFEST的原因有：
// 文件名格式不对， MANIFEST-000001
// 文件太大
// 创建WritableFile失败
bool VersionSet::ReuseManifest(const std::string& dscname, const std::string& dscbase)
{
	if(!options_->resuse_logs)
		return false;
	FileType manifest_type;
	uint64_t manifest_number;
	uint64_t manifest_size;
	if(!ParseFileName(dscbase, &manifest_number, &manifest_type)  ||
		manifest_type != kDescriptorFile  ||
		!env_->GetFileSize(dscname, &manifest_size).ok()  ||
		manifest_size >= TargetFileSize(options_)) // MANIFEST文件超出最大文件大小
	{
		return false;
	}
	
	assert(descriptor_file_ ==NULL);
	assert(descriptor_log_ ==NULL);
	Status r=env_->NewAppendableFile(dscname, &descriptor_file_);
	if(!r.ok())
	{
		Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
		assert(descriptor_file_==NULL);
		return false;
	}
	
	Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
	descriptor_log_=new log::Writer(descriptor_file_, manifest_size);
	return true;
}

// 获得 所有Version 的所有文件序号
void VersionSet::AddLiveFiles(std::set<uint64_t>* live)
{
	for(Version* v=dummy_versions_.next_; v!=&dummy_versions_; v=v->next_)
	{
		for(int level=0; level<config::kNumLevels; level++)
		{
			const std::vector<FileMetaData*>& files = v->files_[level];
			for(size_t i=0; i<files.size(); i++)
				live->insert(files[i]->number);
		}
	}
}

void VersionSet::MarkFileNumberUsed(uint64_t number)
{
	if(next_file_number_ <= number)
		next_file_number_=number+1;
}

void VersionSet::Finalize(Version* v) // 计算下一次compaction的层
{
	int best_level=-1;
	double best_score=-1;
	
	for(int level=0; level<config::kNumLevels-1; level++)
	{
		double score;
		if(level==0)
		{
			// 特殊对待第0层，以文件个数而不是字节数为标准：
			// 第一，写缓存较大，不需要多做0层的compaction
			// 第二，每次read时0层的文件都被合并，所以希望当单个文件大小较小（
			// 可能设置较小的写缓冲、高压缩比、较多的overwrite或deletion）时避免过多的文件
			score=v->files_[level].size() / static_cast<double>(config::kL0_CompactionTrigger);
		}
		else
		{
			const uint64_t level_bytes=TotalFileSize(v->files_[level]);
			score=static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
		}
		
		if(score > best_score)
		{
			best_level=level;
			best_score=score;
		}
	}
	
	v->compaction_level_=best_level;
	v->compaction_score_=best_score;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu)
{
	if(edit->has_log_number_)
	{
		assert(edit->log_number_ >= log_number_);
		assert(edit->log_number_ < next_file_number_);
	}
	else
		edit->SetLogNumber(log_number_);
	
	if(!edit->has_prev_log_number_)
		edit->SetPrevLogNumber(prev_log_number_);
	
	edit->SetNextFile(next_file_number_);
	edit->SetLastSequence(last_sequence_);
	
	// builder模式 构建v,赋给 current_
	Version* v=new Version(this);
	{
		Builder builder(this, current_); 
		builder.Apply(edit);
		builder.SaveTo(v);
	}
	Finalize(v);
	
	// 必要时初始化新的descriptor log文件，通过创建一个包含当前版本的snapshot的临时文件
	std::string new_manifest_file;
	Status s;
	if(descriptor_log_ == NULL)
	{
		assert(descriptor_file_ ==NULL);
		new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
		edit->SetNextFile(next_file_number_);
		s=env_->NewWritableFile(new_manifest_file, &descriptor_file_);
		if(s.ok())
		{
			descriptor_log_=new log::Writer(descriptor_file_);
			s=WriteSnapshot(descriptor_log_); // descriptor_log_即manifest文件 写入当前current_信息
		}
	}
	
	// 在高代价的MANIFEST log的写过程中解锁
	{
		mu->Unlock();
		
		if(s.ok())
		{
			std::string record;
			edit->EncodeTo(&record);
			s=descriptor_log_->AddRecord(record);
			if(s.ok())
				s=descriptor_file_->Sync();
			if(!s.ok())
				Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
		}
		
		// 如果刚刚创建了一个新的descriptor文件，通过写一个新CURRENT文件安装它
		if(s.ok()  &&  !new_manifest_file.empty())
			s=SetCurrentFile(env_, dbname_, manifest_file_number_);
		
		mu->Lock();
	}
	
	if(s.ok())
	{
		AppendVersion(v);
		log_number_ =edit->log_number_;
		prev_log_number_ =edit->prev_log_number_;
	}
	else
	{
		delete v;
		if(!new_manifest_file.empty())
		{
			delete descriptor_log_;
			delete descriptor_file_;
			descriptor_log_ =NULL;
			descriptor_file_ =NULL;
			env_->DeleteFile(new_manifest_file);
		}
	}
	
	return s;
}

Status VersionSet::WriteSnapshot(log::Writer* log)
{
	// 保存metadata
	VersionEdit edit;
	edit.SetComparatorName(icmp_.user_comparator()->Name());
	
	// 保存compaction指针
	for(int level=0; level<config::kNumLevels; level++)
	{
		if(!compact_pointer_[level].empty())
		{
			InternalKey key;
			key.DecodeFrom(compact_pointer_[level]);
			edit.SetCompactPointer(level, key);
		}
	}
	
	for(int level=0; level < config::kNumLevels; level++)
	{
		const std::vector<FileMetaData*>& files=current_->files_[level];
		for(size_t i=0; i<files.size(); i++)
		{
			const FileMetaData* f=files[i];
			edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
		}
	}
	
	std::string record;
	edit.EncodeTo(&record);
	return log->AddRecord(record);
}

Compaction* VersionSet::CompactRange(int level, const InternalKey* begin, const InternalKey* end)
{
	std::vector<FileMetaData*> inputs;
	current_->GetOverlappingInputs(level, begin, end, &inputs);
	if(inputs.empty())
		return NULL;
	
	// 避免range太大时一次compact太多
	// 不包含0层：因为0层会overlap，必须两个文件overlap必须删除一个文件保留一个文件
	if(level >0)
	{
		const uint64_t limit=MaxFileSizeForLevel(options_, level);
		uint64_t total=0;
		for(size_t i=0; i<inputs.size(); i++)
		{
			 uint64_t s=inputs[i]->file_size;
			 total += s;
			 if(total >= limit)
			 {
				 inputs.resize(i+1);
				 break;
			 }
		}
	}
	
	Compaction* c=new Compaction(options_, level);
	c->input_version_ =current_;
	c->input_version_->Ref();
	c->inputs_[0] = inputs;
	SetupOtherInputs(c);
	return c;
}

void VersionSet::SetupOtherInputs(Compaction* c)
{
	const int level = c->level();
	InternalKey smallest, largest;
	GetRange(c->inputs_[0], &smallest, &largest);
	
	current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]); // 获得下一层overlap的文件
	
	InternalKey all_start, all_limit;
	GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit); 
		// 获得覆盖c->inputs_[0]和c->inputs_[1]的最小范围
	
	// 检查是否可以增加level层的inputs的数量，而不改变挑选的level+1层的文件数量
	if(!c->inputs_[1].empty())
	{
		std::vector<FileMetaData*> expanded0;
		current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0); // expanded0是level层覆盖all_start, all_limit的文件
		const int64_t inputs0_size =TotalFileSize(c->inputs_[0]);
		const int64_t inputs1_size =TotalFileSize(c->inputs_[1]);
		const int64_t expanded0_size =TotalFileSize(expanded0);
		if(expanded0.size() > c->inputs_[0].size()  &&  inputs1_size + expanded0_size < ExpandedCompactionByteSizeLimit(options_))
		{
			InternalKey new_start, new_limit;
			GetRange(expanded0, &new_start, &new_limit);
			std::vector<FileMetaData*> expanded1;
			current_->GetOverlappingInputs(level+1, &new_start, &new_limit, &expanded1); // expanded1是level+1层覆盖expanded0文件
			if(expanded1.size() == c->inputs_[1].size())
			{
				Log(options_->info_log,
					"Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
					level,
					int(c->inputs_[0].size()),
					int(c->inputs_[1].size()),
					long(inputs0_size), long(inputs1_size),
					int(expanded0.size()),
					int(expanded1.size()),
					long(expanded0_size), long(inputs1_size));
				smallest =new_start;
				largest=new_limit;
				c->inputs_[0]=expanded0;
				c->inputs_[1]=expanded1;
				GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
			}
		}
	}
	
	// 计算grandparent的与此次compaction重叠的文件集合
	if(level +2 < config::kNumLevels)
		current_->GetOverlappingInputs(level+2, &all_start, &all_limit, &c->grandparents_);
	
	// 更新对level层进行下一次compaction的位置
	compact_pointer_[level]=largest.Encode().ToString();
	c->edit_.SetCompactPointer(level, largest);
}

// 为新compaction选择层和inputs
// 如果没有compaction返回NULL
// 否则返回一个指针，指向一个堆上分配的描述compaction对象，调用者delete
Compaction* VersionSet::PickCompaction()
{
	Compaction* c;
	int level;
	
	// 倾向于一层数据过多时compaction，而不是seek次数多而compaction
	const bool size_compaction =(current_->compaction_score_ >=1);
	const bool seek_compaction =(current_->file_to_compact_ !=NULL);
	if(size_compaction)
	{
		level =current_->compaction_level_;
		assert(level >=0);
		assert(level+1 < config::kNumLevels);
		c=new Compaction(options_, level);
		
		for(size_t i=0; i<current_->files_[level].size(); i++)
		{
			FileMetaData* f=current_->files_[level][i];
			if(compact_pointer_[level].empty()  ||  icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) >0)
			{
				c->inputs_[0].push_back(f);
				break;
			}
		}
		if(c->inputs_[0].empty())
			c->inputs_[0].push_back(current_->files[level][0]);
	}
	else if(seek_compaction)
	{
		level=current_->file_to_compact_level_;
		c =new Compaction(options_, level);
		c->inputs_[0].push_back(current_->file_to_compact_);
	}
	else
		return NULL;
	
	c->input_version_ =current_;
	c->input_version_->Ref();
	
	if(level ==0)
	{
		InternalKey smallest, largest;
		GetRange(c->inputs{[0], &smallest, &largest);
		current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
		assert(!c->inputs_[0].empty());
	}
	
	SetupOtherInputs(c);
	
	return c;
}

Iterator* VersionSet::MakeInputIterator(Compaction* c)
{
	ReadOptions options;
	options.verify_checksums=options_->paranoid_checks;
	options.fill_cache=false;
	
	// 0层文件需要一起merge
	// 对其他层，每层创建一个concatenating迭代器
	const int space=(c->level() ==0 ?  c->inputs_[0].size()+1  :  2);
	Iterator** list=new Iterator*[space];
	int num=0;
	for(int which=0; which <2; which++)
	{
		if(!c->inputs_[which].empty())
		{
			if(c->level() + which ==0)
			{
				const std::vector<FileMetaData*>& files=c->inputs_[which];
				for(size_t i=0; i<files.size(); i++) // 0层的每个文件一个迭代器
					list[num++]=table_cache_->NewIterator(options, files[i]->number, files[i]->file_size);
			}
			else
			{
				// 其他层，每层文件创建concatenating迭代器
				list[num++]=NewTwoLevelIterator(new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]), 
					&GetFileIterator, table_cache_, options);
			}
		}
	}
	
	assert(num <= space);
	Iterator* result = NewMergingIterator(&icmp_, list, num);
	delete[] list;
	return result;
}

bool Compaction::IsTrivialMove() const
{
	const VersionSet* vset=input_version_->vset_;
	
	// 如果有大量overlap的grandparent数据，避免移动
	// 否则，移动将创建一个parent文件，要求一个昂贵的merge
	return (num_input_files(0) ==1  &&  num_input_files(1)==0  &&  // level+1层，没有与level层overlap
			TotalFileSize(grandparents_) <= MaxGrandParentOverlapBytes(vset->options_));
}

bool Compaction::ShouldStopBefore(const Slice& internal_key)
{
	const VersionSet* vset=input_version_->vset_;
	
	// 扫描查找最早的包含key的grandparent文件
	const InternalKeyComparator* icmp=&vset->icmp_;
	while(grandparent_index_ < grandparents_.size()  &&  
		icmp->Compare(internal_key, grandparents_[grandparent_index_]->largest.Encode()) >0) 
	{
		if(seen_key_)
			overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
		grandparent_index_++;
	}
	seen_key_=true;
	
	if(overlapped_bytes_ >MaxGrandParentOverlapBytes(vset->options_))
	{
		// 当前output的overlap太多，开始新的output
		overlapped_bytes_ =0;
		return true;
	}
	else
		return false;
}

void Compaction::ReleaseInputs() {
	if (input_version_ != NULL) {
		input_version_->Unref();
		input_version_ = NULL;
	}
}


