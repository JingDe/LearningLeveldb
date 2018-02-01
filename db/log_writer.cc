
namespace leveldb{

namespace log{

static void InitTypeCrc(uint32_t *type_crc)
{
	for(int i=0; i<=kMaxRecordType; i++)
	{
		char t=static_cast<char>(i);
		type_crc[i]=crc32c::Value(&t, 1);
	}
}

Writer::Writer(WritableFile* dest)
	:dest_(dest),
	block_offset_(0){
	InitTypeCrc(type_crc_);
}

}
}