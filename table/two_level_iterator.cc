

class TwoLevelIterator : public Iterator{
	
	TwoLevelIterator(Iterator* index_iter,
					BlockFunction block_function,
					void* arg,
					const ReadOptions& options);
					
	BlockFunction block_function_;
	void* arg_;
	const ReadOptions options_;
	Status status_;
	IteratorWrapper index_iter_;
	IteratorWrapper data_iter_; // 可以NULL
	std::string data_block_handle_; 
		// 如果 data_iter_ 非空, data_block_handle_ 保存传递给 block_function_ 创建 data_iter_ 的 index_value
};

TwoLevelIterator::TwoLevelIterator(terator* index_iter,
					BlockFunction block_function,
					void* arg,
					const ReadOptions& options)
	: block_function_(block_function),
	arg_(arg),
	options_(options),
	index_iter_(index_iter),
	data_iter_(NULL)
{
	
}

Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_function)(
        void* arg,
        const ReadOptions& options,
        const Slice& index_value), // 函数
    void* arg,
    const ReadOptions& options)
{
	return new TwoLevelIterator(index_iter, block_function, arg, options);
}