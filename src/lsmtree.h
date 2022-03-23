#ifndef __LSMTREE_H__
#define __LSMTREE_H__

#include <vector>
#include <atomic>
#include <algorithm>
#include <mutex>
#include <thread>
#include <string>
#include <math.h>
#include <semaphore.h>
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

const int MAX_COMPACT_LEVELS = 2; //everytime compact 2 levels at most
const int MAX_IMMUTAB_SIZE = 8;
const int MAX_FLUSH_SIZE = 4;
const int MIN_FLUSH_SIZE = 1;

class lsmtree{
    int logidx_;
    wal::walog *wal_;

    memtable *mutab_;
    memtable *immutab_[MAX_IMMUTAB_SIZE];
    std::atomic<int> imsize_;
    std::mutex mutex_;

    thread_pool_t backstage_;

    std::thread *flusher_;
    sem_t sem_free_;
    sem_t sem_occu_;

    std::atomic<bool> running_;
    std::atomic<bool> compacting_;

    versionset versions_;
    std::mutex sstmutex_;

    snapshotlist snapshots_;

    int minor_compact(const int tabnum);
    int major_compact(compaction* c);
    int redolog();
    int make_space();
    void schedule_flush();
    void schedule_compaction();
public:
    lsmtree():
        logidx_(0),
        wal_(nullptr),
        mutab_(nullptr),
        imsize_(0),
        running_(true),
        compacting_(false),
        backstage_(1),
        flusher_(nullptr) {
        mutab_ = new memtable;
        mutab_->ref();
        sem_init(&sem_occu_, 0, 0);
        sem_init(&sem_free_, 0, MAX_IMMUTAB_SIZE);
    }

    ~lsmtree(){
        running_ = false;
        if(flusher_->joinable()){
            flusher_->join();
        }

        if(mutab_){
            delete mutab_;
        }

        if(flusher_!=nullptr){
            delete flusher_;
            flusher_ = nullptr;
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
