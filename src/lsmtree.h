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
#include "walog.h"

typedef std::pair<std::string, std::string> kvpair;

const int TIER_PRI_COUNT = 8;
const int TIER_SST_COUNT(int level);
const int MAX_COMPACT_LEVELS = 2; //everytime compact 2 levels at most

class lsmtree{
    int logidx_;
    wal::walog *wal_;

    memtable *mutab_;
    memtable *immutab_;
    std::mutex mutex_;
    std::condition_variable level0_cv_;

    std::atomic<bool> compacting;
    versionset versions_;
    snapshotlist snapshots_;

    int minor_compact();
    int major_compact(compaction* c);
    int select_overlap(const int ln, std::vector<basetable*> &from, std::vector<basetable*> &to);
    int sweep_space();
    void schedule_compaction();
    int recover();
public:
    lsmtree():
        logidx_(0),
        wal_(nullptr),
        mutab_(nullptr),
        immutab_(nullptr),
        compacting(false){
        mutab_ = new memtable;
        mutab_->ref();
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
    int write(const woptions &opt, const wbatch &bat);

    snapshot * create_snapshot();
    int release_snapshot(snapshot *snap);
};

#endif
