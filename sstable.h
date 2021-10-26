#ifndef _MEMTABLE_H
#define _MEMTABLE_H

#include "sliplist.h"

class sstable{
    int fd;
public:
    int open(const std::string &path){
    }

    int close(){
    }

    int get(const std::string &key, std::string &val){
    }

    int write(const char *sst, int len){
    }
};

#endif
