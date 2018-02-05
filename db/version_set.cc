

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

	VersionSet* vset_;
	
public:
	Builder(VersionSet* vset, Version* base)
		:vset_(vset),
		
};

Status VersionSet::Recover(bool *save_manifest)
{
	struct LogReporter: public log::Reader::Reporter{
		Status* status;
		virtual void Corruption(size_t bytes, const Status& s)
		{
			if (this->status->ok()) *this->status = s;
		}
	};
	
	std::string current;
	Status s=ReadFileToString(env_, CurrentFileName(dbname_), &current); // dbname_/CURRENT 文件
	if(!s.ok())
		return s;
	if(current.empty()  ||  current[current.size()-1] != '\n')
		return Status::Corruption("CURRENT file does not end with newline");
	current.resize(current.size() -1);
	
	std::string dscname=dbname_ + "/" +current;
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
		
	}
}