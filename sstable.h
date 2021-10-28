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

    int put(const std::string &key, std::string &val){
        return 0;
    }

    int reset(const std::vector<std::pair<const char*, const char*> > &tuples){
        return 0;
    }

    int scan(std::function<int(const char*, const char*)> func){
        return 0;
    }
};

#endif
