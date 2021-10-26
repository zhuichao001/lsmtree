#include "lsmtree.h"


int lsmtree::get(const std::string &key, std::string &val){
    if(mutab->get(key, val)==0){
        return 0;
    }
    if(immutab->get(key, val)==0){
        return 0;
    }

    if(levels.size()==0){
        return -1;
    }
    
    for(int j=0; j<levels[0].size(); ++j){
        if(levels[0][j]->get(key, val)==0){
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
    if(levels.size()==0){
        std::vector<sstable*> lev0;
        levels.push_back(lev0);
    }

    sstable *sst = new sstable;
    immutab->scan([=](const std::string &key, const std::string &val) ->int {
        char data[1024];
        sprintf(data, "%d%s%d%s\0", key.size(), key.c_str(), val.size(), val.c_str());
        sst->write(data, strlen(data));
        return 0;
    });

    levels[0].push_back(sst);
    return 0;
}
