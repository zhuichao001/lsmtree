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

typedef std::pair<std::string, std::string> kvpair;

const int TIER_PRI_COUNT = 8;
const int TIER_SST_COUNT(int level);
const int MAX_COMPACT_LEVELS = 2; //everytime compact 2 levels at most

class lsmtree{
    memtable *mutab;
    memtable *immutab;
    pthread_rwlock_t lock;
    std::mutex mutex;
    std::condition_variable level0_cv;

    versionset *versions;

    //serial number
    int sstnumber;

    snapshotlist snapshots;

    char pripath[128];
    char sstpath[128];

    const int sstlimit(const int li){ 
        return 16 * (li<<4); 
    }//leve 1+'s max size

    int minor_compact();
    int major_compact();

    int select_overlap(const int ln, std::vector<basetable*> &from, std::vector<basetable*> &to);

    int ensure_space();

public:
    lsmtree():
        immutab(nullptr),
        sstnumber(0),
        versions(nullptr){
        pthread_rwlockattr_t attr;
        pthread_rwlockattr_init(&attr);
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
        pthread_rwlock_init(&lock, &attr);
        mutab = new memtable;
    }

    ~lsmtree(){
        if(immutab){
            delete immutab;
        }

        if(mutab){
            delete mutab;
        }
    }

    int open(const options *opt, const char *basedir);

    int del(const woptions &opt, const std::string &key);
    int get(const roptions &opt, const std::string &key, std::string &val);
    int put(const woptions &opt, const std::string &key, const std::string &val);
    int write(const wbatch &bat);

    snapshot * get_snapshot();
    int release_snapshot(snapshot *snap);

    int campact(std::string startkey, std::string endkey);
    //iterator *newiterator(); //TODO
};

#endif
