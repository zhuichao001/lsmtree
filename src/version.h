#ifndef _LSMTREE_VERSION_H_
#define _LSMTREE_VERSION_H_

#include <math.h>
#include <algorithm>
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

    std::vector<basetable*> ssts[MAX_LEVELS];

    //compact caused by allowed_seeks become zero
    sstable *hot_sst;

    //max compaction score and corresponding level
    double crownd_score;
    int crownd_level;
public:
    version():
        vset(nullptr),
        refnum(0){
    }

    version(versionset *vs):
        vset(vs),
        refnum(0){
    }

    void ref(){
        ++refnum;
    }

    void unref(){
        assert(refnum>=1);
        if(--refnum==0){
            this->prev->next = this->next;
            this->next->prev = this->prev;
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
    void add(int level, basetable *t){
        assert(level>=0 && level<MAX_LEVELS);
        addfiles[level].push_back(t);
        fprintf(stderr, "add level:%d, key range:[%s, %s]\n", level, t->smallest.c_str(), t->largest.c_str());
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
    
    version verhead_;
    version *current_;

    std::string campact_poles_[MAX_LEVELS]; //next campact start-key for every level

public:
    versionset();

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

    int add_sequence(int cnt){
        last_sequence_ += cnt;
        return last_sequence_;
    }

    version *current(){
        return current_;
    }

    void append(version *ver){
        if(current_!=nullptr && ver!=nullptr){
            fprintf(stderr, "roll up version, %d -> %d\n", current_->ssts[0].size(), ver->ssts[0].size());
        }

        if(current_!=nullptr){
            current_->unref();
        }


        current_ = ver;
        ver->ref();


        //append current_ to tail
        ver->next = &verhead_;
        ver->prev = verhead_.prev;
        verhead_.prev->next = ver;
        verhead_.prev = ver;
    }

    bool need_compact(){
        version *v = current_;
        return v->crownd_score>1 || v->hot_sst!=nullptr;
    }

    compaction *plan_compact();

    void apply(versionedit *edit);
};

#endif
