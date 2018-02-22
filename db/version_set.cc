
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
		base_(base){
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
			*save
	}
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