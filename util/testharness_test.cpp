
#include "testharness.h"

#include<iostream>
#include<typeinfo>


namespace leveldb{

class EmptyClass {};

void testTypeid()
{
	std::cout<<typeid(int).name()<<std::endl;
	std::cout<<typeid(EmptyClass).name()<<std::endl;
}

void testTEST()
{
	std::cout<<"this is testTEST\n";
}

void test_ASSERT()
{
	ASSERT_TRUE(1) << 1;
	ASSERT_EQ(2, 2) << "2==2";
	ASSERT_GE(3, 1) << "3>=1";
}


TEST(EmptyClass, testTypeid) 
{ 
	std::cout<<typeid(*this).name()<<std::endl; 
	testTypeid(); 
}
TEST(EmptyClass, testTEST) { testTEST(); }
TEST(EmptyClass, test_ASSERT) { test_ASSERT(); }

}

int main(int argc, char** argv) {
	return leveldb::test::RunAllTests();
}