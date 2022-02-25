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
#include "tablecache.h"


class sstable: public basetable, public cached {
    int fd;
    bool incache;
    std::multimap<int, int> codemap; //hashcode => datoffset
    int peek(int idxoffset, kvtuple &record) ;
    int endindex();
public:
    sstable(const int lev, const int fileno, const char*leftkey=nullptr, const char *rightkey=nullptr);
    int open();
    int close();
    int reset(const std::vector<kvtuple > &tuples);
public: //implement as cached
    void cache();
    void uncache();
public: //implement as basetable
    int get(const uint64_t seqno, const std::string &key, std::string &val);
    int put(const uint64_t seqno, const std::string &key, const std::string &val, int flag=FLAG_VAL);
    int scan(const uint64_t seqno, std::function<int(const int, const char*, const char*, int)> func);
};

#endif
