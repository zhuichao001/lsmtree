#include "lsmtree.h"

inline int bitsof(const int t){
    int i=1, count=0;
    while(t/i!=0){
        ++count;
        i <<= 1;
    }
    return count;
}

inline static int slot(const char *p){
    static const int BITS = bitsof((TIER_SST_COUNT-1));
    if(p==nullptr){
        return -1;
    }
    return int(*p>>BITS)&(TIER_SST_COUNT-1);
}

/*
inline static int slot(const char *p){
    int hashcode = hash(p, strlen(p));
    return hashcode % TIER_SST_COUNT;
}
*/

int lsmtree::open(const char *basedir){
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

int lsmtree::get(const std::string &key, std::string &val){
    if(mutab->get(key, val)==0){
        return 0;
    }

    if(immutab!=nullptr && immutab->get(key, val)==0){
        return 0;
    }

    if(primarys.size()==0){
        return -1;
    }
    for(int i=0; i<primarys.size(); ++i){
        if(primarys[i]->get(key, val)==0){
            return 0;
        }
    }
    
    for(int i=0; i<levels.size(); ++i){
        for(int j=0; j<levels[i].size(); ++j){
            if(levels[i][j]->get(key, val)==0){
                return 0;
            }
        }
    }
    return -1;
}

int lsmtree::put(const std::string &key, const std::string &val){
    ++verbase;
    if(mutab->put(key, val)<0){
        return -1;
    }

    //fprintf(stderr, "lsmtree::put, key:%s, val:%s\n", key.c_str(), val.c_str());

    /*
    if(mutab->size() == MUTABLE_LIMIT){
        if(immutab!=nullptr){
            fprintf(stderr, "wait until tamper finish compact immutable.\n");
            tamp->wait();
            fprintf(stderr, "tamper has finished compacting.\n");
        }
        immutab = mutab;
        tamp->notify();
        mutab = new memtable;
    } */
    if(mutab->size() == MUTABLE_LIMIT){
        std::lock_guard<std::mutex> lock(mux);
        immutab = mutab;
        sweep();
        delete immutab;
        immutab = nullptr;
        mutab = new memtable;
    }
    return 0;
}

int lsmtree::del(const std::string &key){ //TODO
    ++verbase;
    return mutab->del(key);
}

//transfer hot-data in immumemtable down to sst
int lsmtree::sweep(){
    primarysst *pri = new primarysst;
    char path[128];
    sprintf(path, "%s/%09d.pri\0", pripath, ++prinumber);
    pri->create(path);

    immutab->scan([=](const std::string &key, const std::string &val, int flag) ->int {
        pri->put(key, val, flag);
        return 0;
    });

    primarys.push_back(pri);
    if(primarys.size() >= TIER_PRI_COUNT){
        std::vector<kvtuple> buckets[TIER_SST_COUNT];
        primarys[0]->scan([&](const char *k, const char *v, int flag) ->int {
            buckets[slot(k)].push_back(kvtuple(k, v, flag));
            return 0;
        }); 

        for(int i=0; i<TIER_SST_COUNT; ++i){
            if(!buckets[i].empty()){
                compact(0, i, buckets[i]);
            }
        }
        primarys[0]->remove();
        delete primarys[0];
        primarys.erase(primarys.begin());
    }
    return 0;
}

int lsmtree::compact(int li, int slot, std::vector<kvtuple> &income){
    if(levels.size()<=li){
        std::vector<sstable*> tier;
        for(int i=0; i<TIER_SST_COUNT; ++i){
            sstable *sst = new sstable(li);

            char path[128];
            sprintf(path, "%s/%d/\0", sstpath, li);
            mkdir(path);

            sprintf(path, "%s/%d/%09d.sst\0", sstpath, li, ++sstnumber);
            sst->create(path);

            tier.push_back(sst);
        }
        levels.push_back(tier);
    }

    std::vector<kvtuple> tuples; //for reset current tier
    std::vector<kvtuple> rest;   //for push down next tier
    std::vector<kvtuple> *dest = &tuples;

    int pos = 0;
    std::vector<sstable*> &tier = levels[li];
    tier[slot]->scan([&](const char* k, const char* v, int flag) ->int {
        while(pos<income.size() && strcmp(income[pos].key.c_str(), k)<=0){
            if(tuples.size() >= sstlimit(li)*4/5){
                dest = &rest;
            }
            dest->push_back(income[pos]);
            ++pos;
        }
        dest->push_back(kvtuple(k, v, flag));
        return 0;
    });

    for(int i=pos; i<income.size(); ++i){
        dest->push_back(income[i]);
    }

    tier[slot]->reset(tuples);
    if(!rest.empty()){
        compact(li+1, slot, rest);
    }

    return 0;
}
