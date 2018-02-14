
namespace{

class EmptyIterator: public Iterator{
	
};

}

Iterator* NewEmptyIterator() {
	return new EmptyIterator(Status::OK());
}

Iterator* NewErrorIterator(const Status& status) {
	return new EmptyIterator(status);
}