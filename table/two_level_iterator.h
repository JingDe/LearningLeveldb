

// 返回一个新的两层的迭代器。
// 一个两层的迭代器包含一个索引迭代器，值指向一系列块，每一个块一系列key、value对。
// 返回的迭代器产生所有块的所有key-value对的级联。获得index_iter的所有权，并负责删除。
// 使用一个函数转化index_iter值到相应块的内容上的迭代器。
extern Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_function)(
        void* arg,
        const ReadOptions& options,
        const Slice& index_value), // 函数
    void* arg,
    const ReadOptions& options);