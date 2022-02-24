#ifndef __LSMTREE_H__
#define __LSMTREE_H__

#include <vector>
#include <atomic>
#include <algorithm>
#include <mutex>
#include <string>
#include <math.h>
#include <pthread.h>
#include "memtable.h"
#include "sstable.h"
#include "primarysst.h"
#include "compaction.h"
#include "wbatch.h"
#include "snapshot.h"
#include "threadpool.h"
#include "version.h"
#include "options.h"
#include "util.h"

typedef std::pair<std::string, std::string> kvpair;

const int TIER_PRI_COUNT = 8;
const int TIER_SST_COUNT(int level);
const int MAX_COMPACT_LEVELS = 2; //everytime compact 2 levels at most

class lsmtree{
    memtable *mutab_;
    memtable *immutab_;
    std::mutex mutex_;
    std::condition_variable level0_cv_;

    versionset versions_;
    snapshotlist snapshots_;

    const int sstlimit(const int li){ 
        return 16 * (li<<4); 
    }//leve 1+'s max size

    int minor_compact();
    int major_compact();

    int select_overlap(const int ln, std::vector<basetable*> &from, std::vector<basetable*> &to);

    int sweep_space();

    void schedule_compaction();

public:
    lsmtree():
        mutab_(nullptr),
        immutab_(nullptr){
    }

    ~lsmtree(){
        if(mutab_){
            mutab_->unref();
        }

        if(immutab_){
            immutab_->unref();
        }

    }

    int open(const options *opt, const char *basedir);

    int del(const woptions &opt, const std::string &key);
    int get(const roptions &opt, const std::string &key, std::string &val);
    int put(const woptions &opt, const std::string &key, const std::string &val);
    int write(const wbatch &bat);

    snapshot * create_snapshot();
    int release_snapshot(snapshot *snap);
};

#endif
