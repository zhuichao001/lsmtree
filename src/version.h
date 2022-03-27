#ifndef _LSMTREE_VERSION_H_
#define _LSMTREE_VERSION_H_

#include <math.h>
#include <algorithm>
#include <set>
#include "options.h"
#include "basetable.h"
#include "primarysst.h"
#include "sstable.h"
#include "compaction.h"
#include "tablecache.h"


const int PATH_LEN = 64;

class versionset;

class version {
    friend class versionset;
    friend class compaction;
    version *prev;
    version *next;

    int refnum;
    versionset *vset;
    std::vector<basetable*> ssts[MAX_LEVELS];
    std::mutex mutex_;

    //max compaction score and corresponding level
    double crownd_score;
    int crownd_level;
    //compact caused by allowed_seeks become zero
    basetable *tricky_sst;
public:
    version(versionset *vs);
    ~version();

    version *ref(){
        std::unique_lock<std::mutex> lock{mutex_};
        version *ver = this;
        ++refnum;
        return ver;
    }

    void unref(){
        std::unique_lock<std::mutex> lock{mutex_};
        assert(refnum>=1);
        if(--refnum==0){
            this->prev->next = this->next;
            this->next->prev = this->prev;
            delete this;
        }
    }

    void sort_sst(int level_idx){
        std::sort(ssts[level_idx].begin(), ssts[level_idx].end(), 
            [] (const basetable *a, const basetable *b) { 
                return a->smallest < b->smallest; 
            });
    }

    int get(const uint64_t seqno, const std::string &key, std::string &val);

    void select_sst(const int level, const std::string &start, const std::string &end, std::vector<basetable*> &out);

    void calculate();

    int sst_size(int level){
        return ssts[level].size();
    }

    void print(){
        for(int i=0; i<MAX_LEVELS; ++i){
            for(basetable *t : ssts[i]){
                fprintf(stderr, "  level-%d sst-%d <%s,%s>\n", i, t->file_number, t->smallest.c_str(), t->largest.c_str());
            }
        }
    }
};


class versionedit{
    friend  class versionset;
    std::set<basetable*> delfiles;
    std::vector<basetable*> addfiles[MAX_LEVELS];
public:
    void add(int level, basetable *t){
        assert(level>=0 && level<MAX_LEVELS);
        t->level = level;
        addfiles[level].push_back(t);
    }

    void remove(basetable *t){
        delfiles.insert(t);
    }

    void print(){
        for(basetable *t : delfiles){
            fprintf(stderr, "  ---versionedit del: %d sst-%d <%s,%s>\n", t->level, t->file_number, t->smallest.c_str(), t->largest.c_str());
        }
        for(int i=0; i<MAX_LEVELS; ++i){
            for(basetable *t : addfiles[i]){
                fprintf(stderr, "  +++versionedit add: %d sst-%d <%s,%s>\n", i, t->file_number, t->smallest.c_str(), t->largest.c_str());
            }
        }
    }
};


class versionset {
    std::string dbpath_;
    char metapath_[PATH_LEN];
    const options *opt_;

    int next_fnumber_;
    int last_sequence_;

    int apply_logidx_;
    
    version verhead_;

    version *current_;
    std::mutex mutex_;

    std::string roller_key_[MAX_LEVELS]; //next campact start-key for every level
public:
    tablecache cache_;

    versionset();

    void cachein(basetable *t, bool fixed=false){
        if(t->iscached()){
            cache_.setfixed(std::string(t->path), fixed);
            return;
        }
        t->cache();
        cache_.insert(std::string(t->path), t, fixed);
    }

    void cacheout(basetable *t){
        if(!t->iscached()){
            fprintf(stderr, "warning, cacheout sst-%d but isn't in cache\n", t->file_number);
            return;
        }
        //TODO uncache here
        cache_.evict(std::string(t->path));
    }

    void apply_logidx(int idx){ apply_logidx_ = idx; }

    int apply_logidx(){ return apply_logidx_; }

    int next_fnumber(){ return ++next_fnumber_; }

    int last_fnumber(){ return next_fnumber_; }

    void set_fnumber(int fno){ next_fnumber_ = fno; }

    int last_sequence(){ return last_sequence_; }

    int add_sequence(int cnt){ last_sequence_ += cnt; return last_sequence_; }

    version *current(){ 
        std::unique_lock<std::mutex> lock{mutex_};
        return current_; 
    }

    version *curversion(){ 
        std::unique_lock<std::mutex> lock{mutex_};
        return current_->ref(); 
    }

    void appoint(version *ver);

    bool need_compact(){ return current_->crownd_score>1.0 || current_->tricky_sst!=nullptr; }

    compaction *plan_compact(version *ver);

    version *apply(versionedit *edit);

    int persist(version *ver);

    int recover();
};

#endif
