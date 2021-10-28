#ifndef _UTIL_FIO_H_
#define _UTIL_FIO_H_

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <memory.h>
#include <string>
#include <vector>


int fsize(const int fd);

bool fwriteable(const char *path);

bool seekable(const int fd);

int open_create(const char *path);

int open_append(const char* path);

int open_read(const char* path);

int create(const char* path);

int read_file(const int fd, std::string &data);

int write_file(const int fd, const std::string &data);

int append_file(const int fd, const std::string &data);

int copy_file(const char * src, const char * dst);

bool exist(const char *path);

int mkdir(const char* path);

int ls(const char *path, std::vector<std::string> &files);

#endif