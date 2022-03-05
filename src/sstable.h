#ifndef _SSTABLE_H
#define _SSTABLE_H

#include <map>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include "skiplist.h"
#include "basetable.h"
#include "error.h"
#include "hash.h"
#include "fio.h"
#include "encode.h"


class sstable: public basetable {
    int fd;
    std::multimap<int, rowmeta> codemap; //hashcode => datoffset
    int peek(int idxoffset, kvtuple &record);
    int endindex();
public:
    sstable(const int lev, const int fileno, const char *start=nullptr, const char *end=nullptr);
    int open();
    int close();
    int load();
    int release();
public: //implement as basetable
    int get(const uint64_t seqno, const std::string &key, std::string &val);
    int put(const uint64_t seqno, const std::string &key, const std::string &val, int flag=FLAG_VAL);
    int scan(const uint64_t seqno, std::function<int(const int, const char*, const char*, int)> func);
};

#endif
