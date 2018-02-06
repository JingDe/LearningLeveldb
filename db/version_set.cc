

void Version::Unref()
{
	assert(this != &vset_->dummy_versions_);
	assert(refs_ >=1);
	--refs_;
	if(refs_ == 0)
		delete this;
}


VersionSet::VersionSet(const std::string& dbname, const Options* options, TableCache* table_cache, const InternalKeyComparator* cmp)
	: env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
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
			// 将added files加入已存在的文件，丢掉删除的文件，将结果存到v中
			const std::vector<FileMetaData*>& base_files=base_->files_[level];
			std::vector<FileMetaData*>::const_iterator base_iter=base_files.begin();
			std::vector<FileMetaData*>::const_iterator base_end=base_files.end();
			const FileSet* added=levels_[level].added_files;
			v->files_[level].reserve(base_files.size() + added->size());
			for(FileSet::const_iterator added_iter=added->size(); added_iter!=added->end(); ++added_iter)
			{
				for(std::vector<FileMetaData*>::const_iterator bpos=std::upper_bound(base_iter, base_end, *added_iter, cmp); 
					base_iter != bpos; ++base_iter)
				{
					
				}
			}
			
			// 
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
			s=Status::Corruption("no meta-nextfile entry in descriptor");
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
		
		Finalize(v);
	}
}

void VersionSet::MarkFileNumberUsed(uint64_t number)
{
	if(next_file_number_ <= number)
		next_file_number_=number+1;
}