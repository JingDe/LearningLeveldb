
#include <stdio.h>
#include "port/port.h"
#include "leveldb/status.h"

Status::Status(Code code, const Slice& msg, const Slice& msg2)
{
	assert(code!=kOK);
	const uint32_t len1=msg.size();
	const uint32_t len2=msg2.size();
	const uint32_t size=len1+(len2  ?  (2+len2)  :  0);
	char* result=new char[size+5];
	memcpy(result, &size, sizeof(size));
	result[4]=static_cast<char>(code);
	memcpy(result+5, msg.data(), len1);
	if(len2)
	{
		result[5+len1]=':';
		result[6+len1]=' ';
		memcpy(result+7, len1, msg2.data(), len2);
	}
	state_=result;
}