
#include"skiplist.h"

#include<stdio.h>
#include<stdlib.h>
#include<assert.h>

#include<iostream>

int main()
{
	SkipList sl(4);
	sl.displayList();
	
	int d1=1;
	printf("1,1\n");
	sl.insertNode(1, static_cast<void*>(&d1));sl.displayList();
	printf("2,1\n");
	sl.insertNode(2, static_cast<void*>(&d1));sl.displayList();
	printf("3,1\n");
	sl.insertNode(3, static_cast<void*>(&d1));sl.displayList();
	sl.displayList();
	
	int d2=2;
	printf("1, 2\n");
	sl.insertNode(1, static_cast<void*>(&d2));
	sl.displayList();
	
	sl.deleteNode(1);
	sl.displayList();
	
	sl.deleteNode(1);
	sl.displayList();
	
	int *data=static_cast<int*>(sl.getData(1));
	assert(data==NULL);
	data=static_cast<int*>(sl.getData(2));
	assert(*data==d1);
	
	return 0;
}