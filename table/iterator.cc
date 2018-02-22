
namespace{
	
void Iterator::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2)
{
	assert(func != NULL);
	Cleanup* c;
	if(cleanup_.function == NULL)
		c=&cleanup_;
	else
	{
		c=new Cleanup;
		c->next=cleanup_.next;
		cleanup_.next=c;
	}
	c->function=func;
	c->arg1=arg1;
	c->arg2=arg2;
}

class EmptyIterator: public Iterator{
	
};

}

Iterator* NewEmptyIterator() {
	return new EmptyIterator(Status::OK());
}

Iterator* NewErrorIterator(const Status& status) {
	return new EmptyIterator(status);
}

