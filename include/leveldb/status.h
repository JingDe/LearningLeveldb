
class Status{
public:
	Status(): state_(NULL){}
	
	bool ok() const { return (state_ == NULL); }

	static Status IOError(const Slice& msg, const Slice& msg2=Slice())
	{
		return Status(kIOError, msg, msg2);
	}
	
	static Status NotFound(const Slice& msg, const Slice& msg2=Slice())
	{
		return Status(kNotFound, msg, msg2);
	}
	
	bool InvalidArgument(const Slice& msg, const Slice& msg2=Slice()) const
	{
		return Status(kInvalidArgument, msg, msg2);
	}
	
private:
  // OK status has a NULL state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..]  == message
	const char* state_;

	enum Code{
		kOk=0,
		kNotFound=1,
		kCorruption=2,
		kNotSupported=3,
		kInvalidArgument=4,
		kIOError=5
	};

	Code code() const{
		return (state_==NULL) ? kOk : static_cast<Code>(state_[4]);
	}
	
	Status(Code code, const Slice& msg, const Slice& msg2);
	
};

