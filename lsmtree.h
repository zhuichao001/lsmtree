#ifndef __LSMTREE_H__
#define __LSMTREE_H__

#include <vector>
#include <atomic>
#include <algorithm>
#include "memtable.h"
#include "sstable.h"
#include "primarysst.h"
#include "tamper.h"

typedef std::pair<std::string, std::string> kvpair;

const int MUTABLE_LIMIT = 128;

class lsmtree{
    memtable *mutab;
    memtable *immutab;

    std::vector<primarysst*> primarys; //level 0
    std::vector<std::vector<sstable*> > levels; //level 1+

    //serial number
    int prinumber;
    int sstnumber;

    //record amount limit per sstable
    const int pricount = 16;
    const int sstcount = 8;

    char pripath[128];
    char sstpath[128];

    std::atomic<std::uint64_t> verbase;
    tamper *tamp; //sweep data from immutable to sst

    const int sstlimit(const int li){ return 16 * (li<<4); }//leve 1+'s max size

    int subside();

    int pushdown(int li, int slot, std::vector<std::pair<const char*, const char*> > &income);

public:
    lsmtree():
        immutab(nullptr),
        prinumber(0),
        sstnumber(0),
        verbase(0){
        mutab = new memtable;
        tamp = new tamper(std::bind(&lsmtree::subside, this)); //TODO:multiple threads
    }

    int open(const char *basedir);

    ~lsmtree(){
        if(immutab){
            delete immutab;
        }

        if(mutab){
            delete mutab;
        }
    }

    int get(const std::string &key, std::string &val);

    int put(const std::string &key, const std::string &val);

    int del(const std::string &key);
};

inline int slot(const char *p){
    if(p==nullptr){
        return -1;
    }
    return int(*p>>5)&0x07;
}

#endif
