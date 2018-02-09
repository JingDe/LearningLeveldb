

enum ValueType {
	kTypeDeletion = 0x0,
	kTypeValue = 0x1
};

// kValueTypeForSeek的ValueType使用于，当构建一个ParsedInternalKey对象来seek到特定的sequence number时
// sequence number按由大到小顺序排序，ValueType编码在internal key的sequence number的低8位
// 所有使用最大的ValueType值
static const ValueType kValueTypeForSeek=kTypeValue;


// 从*iter的内容创建一个Table文件，生成的文件将根据meta->number被命名。
// 成功，meta将存储生成table的元数据
extern Status BuildTable(const std::string& dbname,
                         Env* env,
                         const Options& options,
                         TableCache* table_cache,
                         Iterator* iter,
                         FileMetaData* meta);