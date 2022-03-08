#ifndef _MEMTABLE_H
#define _MEMTABLE_H

#include <string>
#include <functional>
#include "type.h"
#include "skiplist.h"

const int MAX_MEMTAB_SIZE = 1<<20; //1MB
const int SKIPLIST_MAX_HEIGHT = 10;
const int BRANCH_SIZE = 16;

typedef struct{
    int logidx;
    uint64_t seqno;
    std::string val;
    uint8_t flag;
} onval;


class memtable{
    skiplist<onval *> table_;
    int size_;
    int refnum;
public:
    memtable();

    void ref(){
        ++refnum;
    }

    void unref(){
        assert(refnum>=1);
        if(--refnum==0){
            delete this;
        }
    }

    int get(const uint64_t seqno, const std::string &key, std::string &val);

    int put(const int logidx, const uint64_t seqno, const std::string &key, const std::string &val, const uint8_t flag=FLAG_VAL);

    int del(const int logidx, const uint64_t seqno, const std::string &key);

    int size(){
        return size_;
    }

    void clear(){
        table_.clear();
    }

    int scan(const uint64_t seqno, std::function<int(int /*logidx*/, uint64_t /*seqno*/, const std::string /*key*/, const std::string /*val*/, int /*flag*/)> visit);

};

#endif
