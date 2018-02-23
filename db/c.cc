
using leveldb::kMajorVersion;
using leveldb::kMinorVersion;


struct leveldb_t              { DB*               rep; };
struct leveldb_iterator_t     { Iterator*         rep; };
struct leveldb_writebatch_t   { WriteBatch        rep; };
struct leveldb_snapshot_t     { const Snapshot*   rep; };
struct leveldb_readoptions_t  { ReadOptions       rep; };
struct leveldb_writeoptions_t { WriteOptions      rep; };
struct leveldb_options_t      { Options           rep; };
struct leveldb_cache_t        { Cache*            rep; };
struct leveldb_seqfile_t      { SequentialFile*   rep; };
struct leveldb_randomfile_t   { RandomAccessFile* rep; };
struct leveldb_writablefile_t { WritableFile*     rep; };
struct leveldb_logger_t       { Logger*           rep; };
struct leveldb_filelock_t     { FileLock*         rep; };

struct leveldb_comparator_t : public Comparator {
	void* state_;
	void (*destructor_)(void*);
	int (*compare_)(
	  void*,
	  const char* a, size_t alen,
	  const char* b, size_t blen);
	const char* (*name_)(void*);

	virtual ~leveldb_comparator_t() {
		(*destructor_)(state_);
	}

	virtual int Compare(const Slice& a, const Slice& b) const {
		return (*compare_)(state_, a.data(), a.size(), b.data(), b.size());
	}

	virtual const char* Name() const {
		return (*name_)(state_);
	}

	// No-ops since the C binding does not support key shortening methods.
	virtual void FindShortestSeparator(std::string*, const Slice&) const { }
	virtual void FindShortSuccessor(std::string* key) const { }
};




static bool SaveError(char** errptr, const Status& s)
{
	assert(errptr != NULL);
	if(s.ok())
		return false;
	else if(*errptr == NULL)
		*errptr=strdup(s.ToString().c_str()); // 复制字符串
	else
	{
		free(*errptr);
		*errptr=strdup(s.ToString().c_str());
	}
	return true;
}

leveldb_t* leveldb_open(const leveldb_options_t* options, const char* name, char** errptr)
{
	DB* db;
	if(SaveError(errptr, DB::Open(options->rep, std::string(name), &db)))
		return NULL;
	leveldb_t* result=new leveldb_t;
	result->rep=db;
	return result;
}

void leveldb_close(leveldb_t* db) {
  delete db->rep;
  delete db;
}

char* leveldb_property_value(leveldb_t* db, const char* propname)
{
	std::string tmp;
	if(db->rep->GetProperty(Slice(propname), &tmp))
		return strdup(tmp.c_str());
	else
		return NULL;
}

leveldb_comparator_t* leveldb_comparator_create()


