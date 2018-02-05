
namespace log{

class Reader{
	
	class Reporter{
		
		virtual void Corruption(size_t bytes, const Status& status) = 0;
	};
};

}