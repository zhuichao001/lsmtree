#ifndef __BPTREE_H__
#define __BPTREE_H__

#include "memtable.h"
#include <vector>

typedef std::pair<std::string, std::string> kvpair;

const int MUTABLE_LIMIT = 128;

class lsmtree{
    memtable *mutab;
    memtable *immutab;
    std::vector<std::vector<sstable*> > levels;
    std::atomic<std::uint64_t> verbase;

    int subside();
public:
    bptree():
        immutab(nullptr),
        verbase(0){
        mutab = new memtable;
    }

    ~bptree(){
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
