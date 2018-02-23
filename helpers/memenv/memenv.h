

// 返回一个新的environment，存储数据在内存中，将所有非文件存储的任务转发给base_env
// 调用方必须删除返回值
Env* NewMemEnv(Env* base_env);