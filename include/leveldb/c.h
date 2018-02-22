
/*
  C bindings for leveldb.  May be useful as a stable ABI that can be
  used by programs that keep leveldb in a shared library, or for
  a JNI api.
  
  静态应用程序二进制接口
  
  约定
  1,只暴露opaque的结构体指针和函数给client；允许改变内部的表示，而不需要重新编译client
  2,没有Slice类型的替代品，调用方需要同时传递指针和长度
  3,错误用null结尾的c string表示。
  4,bool的类型是unsigned char
  5,所有指针参数必须非null
  
*/

#ifdef __cplusplus
extern "C" { // 指示编译器这部分代码按C语言的进行编译
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include "leveldb/export.h"

typedef struct leveldb_t               leveldb_t;
typedef struct leveldb_cache_t         leveldb_cache_t;
typedef struct leveldb_comparator_t    leveldb_comparator_t;
typedef struct leveldb_env_t           leveldb_env_t;
typedef struct leveldb_filelock_t      leveldb_filelock_t;
typedef struct leveldb_filterpolicy_t  leveldb_filterpolicy_t;
typedef struct leveldb_iterator_t      leveldb_iterator_t;
typedef struct leveldb_logger_t        leveldb_logger_t;
typedef struct leveldb_options_t       leveldb_options_t;
typedef struct leveldb_randomfile_t    leveldb_randomfile_t;
typedef struct leveldb_readoptions_t   leveldb_readoptions_t;
typedef struct leveldb_seqfile_t       leveldb_seqfile_t;
typedef struct leveldb_snapshot_t      leveldb_snapshot_t;
typedef struct leveldb_writablefile_t  leveldb_writablefile_t;
typedef struct leveldb_writebatch_t    leveldb_writebatch_t;
typedef struct leveldb_writeoptions_t  leveldb_writeoptions_t;


#ifdef __cplusplus
}  /* end extern "C" */
#endif