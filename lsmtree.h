#ifndef __LSMTREE_H__
#define __LSMTREE_H__

#include <vector>
#include <atomic>
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

    int prinumber;
    int sstnumber;
    const char *pripath;
    const char *sstpath;

    std::atomic<std::uint64_t> verbase;
    tamper *tamp;

    int subside();
public:
    lsmtree():
        immutab(nullptr),
        prinumber(1),
        sstnumber(1),
        pripath("./data/primary/"),
        sstpath("./data/sstable/"),
        verbase(0){
        mkdir(pripath);
        mkdir(sstpath);
        mutab = new memtable;
        tamp = new tamper(std::bind(&lsmtree::subside, this));
    }

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

#endif
