

namespace leveldb{

class PosixLogger: public Logger{
private:
	FILE* file_;
	uint64_t (*gettid_)();

	PosixLogger(FILE* f, uint64_t (*gettid)()): file_(f), gettid_(gettid)
	{}
	

};

}