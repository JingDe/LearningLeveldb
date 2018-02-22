
// Env

#ifndef LEARNINGLEVELDB_ENV_H_
#define LEARNINGLEVELDB_ENV_H_

#include<cstdio>
#include<vector>
#include<string>
#include<sys/types.h>
#include<dirent.h>
#include<error.h>
#include<cstring>

#include<unistd.h>
#include<sys/syscall.h>

/*
struct dirent{
	ino_t d_ino; //inode序号
	off_t d_off; //同telldir，返回当前directory stream的位置
	unsigned d_reclen; // 这条记录的长度
	unsigned char d_type; //文件类型，块设备、字符设备、目录、命名管道、符号链接、普通文件、UNIX域套接字
	char d_name[256]; //文件名
};
*//*
namespace learningleveldb{



//dir是完整路径名，获得目录下所有文件名到files中
void MyGetChildren(const char* path, std::vector<std::string> *files)
{
	DIR *dir;
	dir=opendir(path);
	struct dirent *dent;
	if(dir==NULL)
	{
		fprintf(stderr, "opendir '%s' error: %s\n", path, strerror(errno));
		return ;
	}
	
	while(dent=readdir(dir))
	{
		if(strcmp(dent->d_name, ".")==0   ||  strcmp(dent->d_name, "..")==0)
			continue;
		files->push_back(std::string(dent->d_name));
	}
	closedir(dir);
}

//file是待删除的文件的完整路径名
void MyDeleteFile(std::string file)
{
	int ret=remove(file.c_str());
	if(ret<0)
	{
		fprintf(stderr, "delete file %s error: %s\n", file.c_str(), strerror(errno));
	}
}



} // namespace learningleveldb
*/

namespace leveldb{
	
class FileLock;

class Env{
public:
	Env() {}
	virtual ~Env();
	
	virtual Status NewWritableFile(const std::string& fname, WritableFile** result) =0;
	
	virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) = 0;
};

class WritableFile{
public:
	WritableFile() {}
	virtual ~WritableFile();
	
private:
	WritableFile(const WritableFile&);
	void operator=(const WritableFile&);
};

class Logger{
	virtual void Logv(const char* format, va_list ap) = 0;
};

	
class FileLock{
public:
	FileLock() {}
	virtual ~FileLock();
private:
	FileLock(const FileLock&);
	void operator=(const FileLock&);
};


}
#endif
