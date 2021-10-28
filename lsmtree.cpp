#include "lsmtree.h"


int lsmtree::get(const std::string &key, std::string &val){
    if(mutab->get(key, val)==0){
        return 0;
    }
    if(immutab->get(key, val)==0){
        return 0;
    }

    if(primarys.size()==0){
        return -1;
    }

    for(int i=0; i<primarys.size(); ++i){
        if(primarys[i]->get(key,val)==0){
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

    if(mutab->size() == MUTABLE_LIMIT){
        if(immutab!=nullptr){
            tamp->wait();
            fprintf(stderr, "wait until tamper finish compact immutable.\n");
            tamp->wait();
            fprintf(stderr, "tamper has finished compacting.\n");
        }
        immutab = mutab;
        tamp->notify();

        mutab = new memtable;
    }
    return 0;
}

int lsmtree::del(const std::string &key){
    ++verbase;
    return mutab->del(key);
}

int lsmtree::subside(){
    primarysst *pri = new primarysst;
    char path[128];
    sprintf(path, "%s/%09d.pri\0", pripath, ++prinumber);
    pri->create(path);

    immutab->scan([=](const std::string &key, const std::string &val) ->int {
        return pri->put(key, val);
    });

    primarys.push_back(pri);
    if(primarys.size() >= pricount){
        std::vector<std::pair<const char*, const char*> > buckets[8];
        primarys[0]->scan([&](const char *k, const char *v) ->int {
            buckets[slot(k)].push_back(std::make_pair(k, v));
            return 0;
        }); 

        for(int i=0; i<8; ++i){
            pushdown(0, i, buckets[i]);
        }
        primarys[0]->remove();
        delete primarys[0];
        primarys.erase(primarys.begin());
    }
    return 0;
}

int lsmtree::pushdown(int li, int slot, std::vector<std::pair<const char*, const char*> > &income){
    if(levels.size()<=li){
        std::vector<sstable*> tier;
        for(int i=0; i<sstcount; ++i){
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

    std::vector<std::pair<const char*, const char*> > tuples; //for reset current tier
    std::vector<std::pair<const char*, const char*> > rest;   //for push down next tier
    std::vector<std::pair<const char*, const char*> > *dest = &tuples;

    int pos = 0;
    std::vector<sstable*> &tier = levels[li];
    tier[slot]->scan([&](const char* k, const char* v) ->int {
       while(pos<income.size() && strcmp(k, income[pos].first)>0){
           if(tuples.size() >= sstlimit(li)*4/5){
               dest = &rest;
           }
           dest->push_back(income[pos]);
           ++pos;
       }
       dest->push_back(std::make_pair(k,v));
       return 0;
    });

    tier[slot]->reset(tuples);
    if(!rest.empty()){
        pushdown(li+1, slot, rest);
    }

    return 0;
}
