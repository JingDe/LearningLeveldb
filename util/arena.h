#ifndef LEARNING_LEVELDB_ARENA_H
#define LEARNING_LEVELDB_ARENA_H

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "port/port.h"

namespace leveldb{

class Arena{
public:
	Arena();
	~Arena();
	
	char* Allocate(size_t bytes); // 返回指针指向新分配的bytes大小内存块
	
	char* AllocateAligned(size_t bytes); // 分配的内存通过malloc获得正常的对齐保证
	
	size_t MemoryUsage() const
	{
		return reinterpret_cast<uintptr_t>(memory_usage_.NoBarrier_Load());
	}
	
private:
	char* AllocateFallback(size_t bytes);
	char* AllocateNewBlock(size_t block_bytes);
	
	char* alloc_ptr_;
	size_t alloc_bytes_remaining_;
	
	std::vector<char*> blocks_; // new[] 分配的内存块的数组
	
	port::AtomicPointer memory_usage_; // 总的内存使用
	
	Arena(const Arena&);
	void operator=(const Arena&);
};

inline char* Arena::Allocate(size_t bytes)
{
	assert(bytes>0);
	if(bytes <= alloc_bytes_remaining_)
	{
		char* result=alloc_ptr_;
		alloc_ptr_+=bytes;
		alloc_bytes_remaining_ -= bytes;
		return result;
	}
	return AllocateFallback(bytes);
}

}

#endif