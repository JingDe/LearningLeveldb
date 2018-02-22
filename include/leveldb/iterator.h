
class Iterator{

	typedef void (*CleanupFunction)(void* arg1, void* arg2);
	void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);
	
private:
	struct Cleanup{
		CleanupFunction function;
		void* arg1;
		void* arg2;
		Cleanup* next;
	};
	Cleanup cleanup_;
	
};