#ifndef LEARNING_LEVELDB_TESTHARNESS_H_
#define LEARNING_LEVELDB_TESTHARNESS_H_


#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "util/random.h"

namespace leveldb{
namespace test{

extern int RunAllTests();

extern int RandomSeed();


#define TCONCAT(a, b) TCONCAT1(a, b)
#define TCONCAT1(a, b)  a##b   //字符串组合

// 创建一个类，将 _RunIt函数注册， _RunIt函数调用_Run()函数，使用TEST宏的用户提供_Run()函数体
#define TEST(base, name) \
class TCONCAT(_Test_, name) : public base{ \
public:  \
	void _Run();  \
	static void _RunIt() {  \
		TCONCAT(_Test_, name) t;  \
		/*printf(stderr, "%s%s, %s\n", _Test_, name, _Test_ignored_);*/  \
		t._Run();  \
	}  \
};\
bool TCONCAT(_Test_ignored_, name) =  \
	::leveldb::test::Register(#base, #name, &TCONCAT(_Test_, name)::_RunIt);  \
void TCONCAT(_Test_, name)::_Run() // 后接 函数体


extern bool Register(const char* base, const char* name, void (*func)());

}
}

#endif