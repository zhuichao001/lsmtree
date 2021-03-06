#ifndef _PRIMARY_SSTABLE_H
#define _PRIMARY_SSTABLE_H

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <map>
#include "basetable.h"
#include "error.h"
#include "hash.h"
#include "fio.h"


class primarysst: public basetable{
    char *mem;
    std::multimap<int, kvtuple> codemap; //hashcode => kvtuple(in order to speed up)
    int peek(int idxoffset, kvtuple &record);
public:
    primarysst(const int fileno, const char *start=nullptr, const char *end=nullptr, int keynum=0);
    ~primarysst();
    int open();
    int close();
    int load();
    int release();
public:
    int get(const uint64_t seqno, const std::string &key, kvtuple &res);
    int get(const uint64_t seqno, const std::string &key, std::string &val);
    int put(const uint64_t seqno, const std::string &key, const std::string &val, int flag=FLAG_VAL);
    int scan(const uint64_t seqno, std::function<int(const int, const char*, const char*, int)> func);
};

#endif
