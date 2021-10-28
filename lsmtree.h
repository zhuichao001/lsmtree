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

    int prinumber;
    int sstnumber;
    char pripath[128];
    char sstpath[128];

    std::atomic<std::uint64_t> verbase;
    tamper *tamp;

    const int pricount = 16;
    const int sstcount = 8;
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
        tamp = new tamper(std::bind(&lsmtree::subside, this));
    }

    int open(const char *basedir){
        sprintf(pripath, "%s/pri/\0", basedir);
        sprintf(sstpath, "%s/sst/\0", basedir);
        if(!exist(basedir)){
            mkdir(pripath);
            mkdir(sstpath);
        }

        std::vector<std::string> files;
        ls(pripath, files);
        for(auto path: files){
            primarysst *pri = new primarysst;
            pri->load(path.c_str());
            primarys.push_back(pri);
        }

        files.clear();
        ls(sstpath, files);
        std::sort(files.begin(), files.end());
        for(auto path: files){
            int tierno = atoi(path.c_str()+strlen(sstpath));
            for(int i=0; i<=tierno; ++i){
                std::vector<sstable*> tier;
                levels.push_back(tier);
            }
            sstable *sst = new sstable(tierno);
            sst->load(path.c_str());
            levels[tierno].push_back(sst);
        }
        return 0;
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

inline int slot(const char *p){
    if(p==nullptr){
        return -1;
    }
    return int(*p>>5)&0x07;
}

#endif
