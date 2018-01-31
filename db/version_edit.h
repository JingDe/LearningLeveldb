
class VersionEdit{

public:
	VersionEdit() { Clear(); }
	~VersionEdit() {}
	
	void Clear();
	
private:

	std::string comparator_;
};