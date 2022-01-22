#ifndef __LSMTREE_H__
#define __LSMTREE_H__

#include <vector>
#include <atomic>
#include <algorithm>
#include <mutex>
#include <math.h>
#include "memtable.h"
#include "sstable.h"
#include "primarysst.h"
#include "tamper.h"
#include "wbatch.h"
#include "snapshot.h"

typedef std::pair<std::string, std::string> kvpair;

const int MAX_LEVELS = 8;
const int MUTABLE_LIMIT = 1<<20; //2MB size FIXED
const int TIER_PRI_COUNT = 8;
const int TIER_SST_COUNT(int level);
const int MAX_COMPACT_LEVELS = 2; //everytime compact 2 levels at most

class lsmtree{
    memtable *mutab;
    memtable *immutab;
    std::mutex mux;

    std::vector<basetable*> levels[MAX_LEVELS];

    //serial number
    int sstnumber;

    char pripath[128];
    char sstpath[128];

    std::atomic<std::uint64_t> verno;
    tamper *tamp; //sweep data from immutable to sst

    const int sstlimit(const int li){ 
        return 16 * (li<<4); 
    }//leve 1+'s max size

    void swapmutab(); //swap out mutab

    int sweep();
    int minor_compact();
    int major_compact(int ln);
    int select_overlap(const int ln, std::vector<basetable*> &from, std::vector<basetable*> &to);
    void sort_sst(int level_idx){
        std::sort(levels[level_idx].begin(), levels[level_idx].end(), [] (const basetable *a, const basetable *b) { 
                return a->smallest < b->smallest; });
    }

public:
    lsmtree():
        immutab(nullptr),
        sstnumber(0),
        verno(0){
        mutab = new memtable;
        tamp = new tamper(std::bind(&lsmtree::sweep, this)); //TODO:multiple threads
    }

    ~lsmtree(){
        if(immutab){
            delete immutab;
        }

        if(mutab){
            delete mutab;
        }
    }

    int open(const char *basedir);

    int del(const std::string &key);

    int get(const std::string &key, std::string &val);

    int put(const std::string &key, const std::string &val);

    int write(const wbatch &bat);

    snapshot * create_snapshot();

    int release_snapshot();
};

#endif
