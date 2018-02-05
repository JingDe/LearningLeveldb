#include"leveldb/env.h"

namespace leveldb{

FileLock::~FileLock(){
}

static Status DoWriteStringToFile(Env* env, const Slice& data, const std::string& fname, bool should_sync)
{
	WritableFile* file;
	Status s=env->NewWritableFile(fname, &file);
	if(!s.ok())
		return s;
	s=file->Append(data);
	if(s.ok()  &&  should_sync)
		s=file->Sync();
	if(s.ok())
		s=file->Close();
	delete file;
	if(!s.ok())
		env->DeleteFile(fname);
	return s;
}

Status WriteStringToFileSync(Env* env, const Slice& data, const std::string& fname)
{
	return DoWriteStringToFile(env, data, fname, true);
}

}