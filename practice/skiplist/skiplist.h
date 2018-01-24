#ifndef SKIPLIST_H
#define SKIPLIST_H

#include<stdio.h>
#include<stdlib.h>

struct Node{
	int key;
	void* data;
	int level;
	Node **next_nodes;
};

class SkipList{
public:
	SkipList(int level);
	~SkipList();
	
	void insertNode(int key, void *data);
	void deleteNode(int key);
	void* getData(int key);
	void displayList();

private:
	int RandomLevel();

	int MAX_LEVEL;
	Node *head;
	Node *tail;
};

SkipList::SkipList(int level)
{
	MAX_LEVEL=level;
	head=new Node();
	head->level=MAX_LEVEL;
	head->next_nodes=new Node*[MAX_LEVEL];
	
	tail=new Node();
	tail->level=MAX_LEVEL;
	tail->next_nodes=NULL;
	
	for(int i=0; i<MAX_LEVEL; i++)
		head->next_nodes[i]=tail;
}

SkipList::~SkipList()
{
	Node* curr=NULL;
	
	while(head->next_nodes[0]!=tail)
	{
		curr=head->next_nodes[0];
		head->next_nodes[0]=curr->next_nodes[0];
		delete []curr->next_nodes;
		delete curr;
	}
	
	delete []head->next_nodes;
	delete head;
	delete tail;
}

void SkipList::insertNode(int key, void *data)
{
	Node* updates[MAX_LEVEL];
	int level=RandomLevel();
	printf("level=%d\n", level);
	
	for(int i=0; i<=level; i++)
	{
		Node* curr=head;
		if(curr->next_nodes[i]==tail  ||  curr->next_nodes[i]->key>key)
			updates[i]=curr;
		else
		{
			while(curr->next_nodes[i]!=tail  &&  curr->next_nodes[i]->key<key)
				curr=curr->next_nodes[i];
			if(curr->next_nodes[i]!=tail  &&  curr->next_nodes[i]->key==key)
			{
				curr->next_nodes[i]->data=data;
				printf("insertNode: return\n");
				return ;
			}
			updates[i]=curr;
		}
		printf("i=%d, updates[i]=%p\n", i, updates[i]);
	}
	
	Node *n=new Node;
	n->key=key;
	n->data=data;
	n->level=level;
	n->next_nodes=new Node*[level+1]; // 不能少	
	
	
	for(int i=0; i<=level; i++)
	{
		n->next_nodes[i]=updates[i]->next_nodes[i];
		updates[i]->next_nodes[i]=n;
	}
}

void SkipList::deleteNode(int key)
{
	Node* updates[MAX_LEVEL];
	
	
	for(int i=0; i<MAX_LEVEL; i++)
	{
		Node* curr=head;
		if(curr->next_nodes[i]==tail  ||  curr->next_nodes[i]->key==key)
			updates[i]=curr;
		else
		{
			while(curr->next_nodes[i]!=tail  &&  curr->next_nodes[i]->key<key)
				curr=curr->next_nodes[i];
			if(curr->next_nodes[i]!=tail  &&  curr->next_nodes[i]->key==key)
				updates[i]=curr;
			else 
				updates[i]=NULL;
		}
		printf("i=%d, updates[i]=%p\n", i, updates[i]);
	}
	
	Node *tmp=NULL;
	for(int i=0; i<MAX_LEVEL; i++)
	{
		if(updates[i])
		{
			tmp=updates[i]->next_nodes[i];
			updates[i]->next_nodes[i]=tmp->next_nodes[i];
		}
	}
	if(tmp)
	{
		delete []tmp->next_nodes;
		delete tmp;
	}
}

/* 另种写法
void* SkipList::getData(int key)
{
	Node* curr=head;
	for(int i=MAX_LEVEL-1; i>=0; i--)
	{
		if(curr->next_nodes[i]==tail  ||  curr->next_nodes[i]->key>key)
			continue;
		else
		{
			while(curr->next_nodes[i]!=tail  &&  curr->next_nodes[i]->key<key)
				curr=curr->next_nodes[i];
			if(curr->next_nodes[i] !=tail  &&  curr->next_nodes[i]->key==key)
				return curr->next_nodes[i]->data;
		}
	}
	return nullptr;
}
*/

void *SkipList::getData(int key)
{
	Node* x=head;
	for(int i=MAX_LEVEL-1; i>=0; i--)
	{
		Node* curr=x->next_nodes[i];
		while(curr!=tail  &&  curr->key<key)
		{
			x=curr;
			curr=curr->next_nodes[i];
		}
		
		if(curr!=tail  &&  curr->key==key)
			return curr->data;
	}
	return nullptr;
}



void SkipList::displayList()
{
	Node* curr=head->next_nodes[0];
	while(curr!=tail)
	{
		printf("key=%d data=%p, *data=%d level=%d\n", curr->key, curr->data, *(static_cast<int*>(curr->data)), curr->level);
		curr=curr->next_nodes[0];
	}
	puts("");
}

int SkipList::RandomLevel()
{
	int level=0;
	while(rand() % 2  &&  level<MAX_LEVEL-1)
		++level;
	return level;
}

#endif
