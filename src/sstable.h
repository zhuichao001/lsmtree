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
#include "types.h"


class sstable: public basetable{
    int fd;
    std::multimap<int, int> codemap; //hashcode => datoffset
    int peek(int idxoffset, kvtuple &record) ;
    int endindex();
public:
    sstable(const int lev);

    int create(const char *path);

    int load(const char *path);

    int close();

    int reset(const std::vector<kvtuple > &tuples);
public:
    int get(const uint64_t seqno, const std::string &key, std::string &val);

    int put(const uint64_t seqno, const std::string &key, const std::string &val, int flag=0);

    int scan(const uint64_t seqno, std::function<int(const char*, const char*, int)> func);
};

sstable *create_sst(int level, int filenumber);

#endif
