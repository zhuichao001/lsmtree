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
    sprintf(path, "%s/%d.pri\0", pripath, prinumber);
    pri->create(path);

    immutab->scan([=](const std::string &key, const std::string &val) ->int {
        return pri->put(key, val);
    });

    primarys.push_back(pri);
    //TODO if primarys too large
    return 0;
}
