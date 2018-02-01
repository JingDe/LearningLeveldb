#include<iostream>
#include<string>
#include <stdint.h>

char* EncodeVarint32(char* dst, uint32_t v) {

  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  //char* ptr =dst;
  static const int B = 128;
  if (v < (1<<7)) {
    *(ptr++) = v;
    std::cout<<"1: "<<v<<std::endl;
    //std::cout<<*(ptr-1)<<std::endl;
    std::cout<<static_cast<int>(*(ptr-1))<<std::endl;
  } else if (v < (1<<14)) {
    *(ptr++) = v | B;
    *(ptr++) = v>>7;
    std::cout<<"2: "<< (v | B) <<"\t"<< (v>>7) <<std::endl;
    std::cout<<static_cast<int>(*(ptr-2))<<"\t"<<static_cast<int>(*(ptr-1))<<std::endl;
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
    std::cout<<"3: "<< (v | B) <<"\t"<< ((v>>7) | B )<<"\t"<< (v>>14) <<std::endl;
    std::cout<<static_cast<int>(*(ptr-3))<<"\t"<<static_cast<int>(*(ptr-2))<<"\t"<<static_cast<int>(*(ptr-1))<<std::endl;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
    std::cout<<"4: "<< (v | B) <<"\t"<< ((v>>7) | B )<<"\t"<< ((v>>14) | B) <<"\t"<< (v>>21)<<std::endl;
    std::cout<<static_cast<int>(*(ptr-4))<<"\t"<<static_cast<int>(*(ptr-3))<<"\t"<<static_cast<int>(*(ptr-2))<<"\t"<<static_cast<int>(*(ptr-1))<<std::endl;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

void test(int v)
{
	std::cout<<v<<std::endl;
	std::string record;
	
	char buf[5];
	char* ptr=EncodeVarint32(buf, v);
	record.append(buf, ptr-buf);
	//std::cout<<record<<std::endl;
	std::cout<<std::endl;
}

int main()
{
	test( (1<<7)-1 );
	test( (1<<14)-1 );
	test( (1<<21)-1 );
	test( (1<<28)-1 );
	
	return 0;
}
