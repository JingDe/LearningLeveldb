
#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb{

class Arena;

template<typename Key, class Comparator>
class SkipList{
private:
	struct Node;
	
public:
	explicit SkipList(Comparator cmp, Arena *arena);
	
	void Insert(const Key& key);
	
	bool Contains(const Key& key) const;
	
	class Iterator{};
	
private:
	enum {kMaxHeight=12}; // 代替 static const kMaxHeight=12; 编译器不允许class内部初始化static常量
	
	Comparator const compare_;
	Arena* const arena_;
	
	Node* const head_;
	
	port::AtomicPointer max_height_; // 跳表层数，允许动态更改
	
	inline int GetMaxHeight() const
	{
		return static_cast<int>(reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
	}
	
	Random rand_;
	
	Node* NewNode(const Key& key, int height);
	int RandomHeight();
	bool Equal(const Key& a, const Key& b) const
	{
		return (compare_(a, b)==0);
	}	
	
	bool KeyIsAfterNode(const Key& key, Node* n) const; // 当key大于n结点的Key返回true
	
	// 返回等于key或者key后面的结点
	// 当prev不空，prev[level]返回key结点在level层的前驱结点
	Node* FindGreaterOrEqual(const Key* key, Node** prev) const; 
	
	Node* FindLessThan(const Key& key) const; // 返回最后一个小于key的结点
	
	Node* FindLast() const; // 返回最后一个节点
	
	// 禁止拷贝
	SkipList(const SkipList&); 
	void operator=(const SkipList&);
};


template<typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node
{
	explicit Node(const Key& k):key(k) {}
	
	Key const key;
	
	Node* Next(int n) // 获得结点在第n层的后继结点
	{
		assert(n>=0);
		return reinterpret_cast<Node*>(next_[n].Acquire_Load());
	}
	void SetNext(int n, Node* x)
	{
		assert(n>=0);
		next_[n].Release_Store(x);
	}
	Node* NoBarrier_Next(int n) // 获得结点在第n层的后继结点
	{
		assert(n>=0);
		return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
	}
	void NoBarrier_SetNext(int n, Node* x)
	{
		assert(n>=0);
		next_[n].NoBarrier_Store(x);
	}
	
private:
	port::AtomicPointer next_[1]; // 存储结点在每一层的后继结点
};

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(const Key& key, int height)
{
	char *mem=arena_->AllocateAligned(sizeof(Key) + sizeof(port::AtomicPointer) * (height-1)); // 为什么-1
	return new (mem) Node(key);
}


template<typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight()
{
	static const unsigned int kBranching=4;
	int height=1; // 最小层数是1
	while( (rnd_.Next()  % kBranching)==0  &&  height<kMaxHeight) // 层数增加的概率为1/4
		height++;
	assert(height > 0);
	assert(height <= kMaxHeight);
	return height;
}

template<typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const
{
	return compare_(key, n->key)>0;
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key, Comparator>::FindGreaterOrEqual(const Key* key, Node** prev) const
{
	Node *x=head_;
	int level=GetMaxHeight() -1;
	while(true)
	{
		Node* next=x->Next(level);
		if(KeyIsAfterNode(key, next))
			x=next;
		else
		{
			if(prev)
				prev[level]=x;
			if(level==0)
				return next;
			else
				level--;
		}
	}
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key, Comparator>::FindLessThan(const Key& key) const
{
	Node* x=head_;
	int level=GetMaxHeight()-1;
	while(true)
	{
		Node* next=x->Next(level);
		if(next==NULL  ||  compare_(next->key, key) >= 0)
		{
			if(level==0)
				return x;
			else
				level--;
		}
		else
		{
			x=next;
		}
	}
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key, Comparator>::FindLast() const
{
	Node* x=head_;
	int level=GetMaxHeight()-1;
	while(true)
	{
		Node* next=x->Next(level);
		if(next==NULL)
		{
			if(level==0)
				return x;
			else
				level--;
		}
		else
		{
			x=next;
		}
	}
}

template<typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
	:compare_(cmp),
	arena_(arena),
	head_(NewNode(0, kMaxHeight)),
	max_height_(reinterpret_cast<void*>(1)), //第0层
	rnd_(0xdeadbeef) {
	for(int i=0; i<kMaxHeight; i++)
	{
		head_->SetNext(i, NULL);
	}
}

template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key)
{
	Node* prev[kMaxHeight];
	Node* x=FindGreaterOrEqual(key, prev);
	
	assert(x==NULL  ||  !Equal(key, x->key));
	
	
	int height=RandomHeight();
	if(height>GetMaxHeight())
	{
		for(int i=GetMaxHeight(); i<height; i++)
			prev[i]=head_;
		max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
	}
	
	Node* newNode=NewNode(key, height);
	for(int i=0; i<height; i++)
	{
		newNode->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
		prev[i]->SetNext(i, newNode);
	}
}

template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::Contains(const Key& key) const
{
	Node* x=FindGreaterOrEqual(key, NULL);
	if(x  &&  Equal(key, x.key))
		return true;
	return false;
}

	
}