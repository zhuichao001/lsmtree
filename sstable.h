#ifndef _SSTABLE_H
#define _SSTABLE_H

#include "skiplist.h"

class sstable{
    int fd;
public:
    int open(const std::string &path){
        return 0;
    }

    int close(){
        return 0;
    }

    int get(const std::string &key, std::string &val){
        return 0;
    }

    int write(const char *sst, int len){
        return 0;
    }
};

#endif
