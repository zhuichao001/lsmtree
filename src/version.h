#ifndef _LSMTREE_VERSION_H_
#define _LSMTREE_VERSION_H_

#include <math.h>
#include <set>
#include "options.h"
#include "types.h"
#include "basetable.h"
#include "primarysst.h"
#include "sstable.h"
#include "compaction.h"
#include "tablecache.h"

class versionset;

int max_level_size(int ln);

class version {
    friend class versionset;
    friend class compaction;
    version *prev;
    version *next;

    versionset *vset;
    int refnum;
    bool compacting;

    std::vector<basetable*> ssts[MAX_LEVELS];

    //compact caused by allowed_seeks become zero
    sstable *hot_sst;

    //max compaction score and corresponding level
    double crownd_score;
    int crownd_level;
public:
    void ref(){
        ++refnum;
    }

    void unref(){
        assert(refnum>=1);
        if(--refnum==0){
            delete this;
        }
    }

    void sort_sst(int level_idx){
        std::sort(ssts[level_idx].begin(), ssts[level_idx].end(), [] (const basetable *a, const basetable *b) { 
                return a->smallest < b->smallest; });
    }

    int get(const uint64_t seqno, const std::string &key, std::string &val);

    void calculate();
};


class versionedit{
private:
    friend  class versionset;
    std::set<basetable*> delfiles;
    std::vector<basetable*> addfiles[MAX_LEVELS];
public:
    void add(basetable *t){
        addfiles[t->getlevel()].push_back(t);
    }

    void remove(basetable *t){
        delfiles.insert(t);
    }
};


class versionset {
private:
    std::string dbpath_;
    const options *opt_;
    tablecache *cache_;

    int next_fnumber_; //TODO: atomic
    int last_sequence_;
    
    version *current_;

    std::string campact_poles_[MAX_LEVELS]; //next campact start-key for every level

public:
    int next_fnumber(){
        return ++next_fnumber_;
    }

    int last_fnumber(){
        return next_fnumber_;
    }

    void set_fnumber(int fno){
        next_fnumber_ = fno;
    }

    int last_sequence(){
        return last_sequence_;
    }

    void add_sequence(int cnt){
        last_sequence_ += cnt;
    }

    version *current(){
        return current_;
    }

    //TODO: for manual compaction
    int compact_range(const int level, const std::string &startkey, const std::string &endkey){
        return 0;
    }

    bool need_compact(){
        version *v = current_;
        return v->crownd_score>1 || v->hot_sst!=nullptr;
    }

    compaction *plan_compact();

    void apply(versionedit *edit);
};

#endif
