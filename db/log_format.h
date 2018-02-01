
namespace leveldb{

namespace log{

enum RecordType{
	kZeroType=0,
	kFullType=1,
	kFirstType=2,
	kMiddleType=3,
	kLastType=4
};
static const int kMaxRecordType=kLastType;

}
}