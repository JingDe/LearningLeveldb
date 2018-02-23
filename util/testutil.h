
class ErrorEnv: public EnvWrapper{
	bool writable_file_error_;
	int num_writable_file_errors_;
	
	virtual Status NewWritableFile(const std::string& fname, WritableFile** result)
	{
		if(writable_file_error_)
		{
			++num_writable_file_errors_;
			*result=NULL;
			return 
		}
		return target()->NewWritableFile(fname, result);
	}
};