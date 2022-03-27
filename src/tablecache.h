#ifndef _LSMTREE_TABLECACHE_H_
#define _LSMTREE_TABLECACHE_H_

#include "lru.h"

class cached{
public:
    virtual bool cache() = 0;
    virtual void uncache() = 0;
};

class tablecache{
    LRUCache<std::string, cached* > lru;
public:
    tablecache():
        lru(960){ //such sstables will be cached at most
    }

    int insert(const std::string &k, cached* v, bool fixed){
        if(lru.exist(k)){
            fprintf(stderr, "warning, ignore cache %s exist\n", k.c_str());
            //lru.display();
            return -1;
        }
        fprintf(stderr, "LRU put cache %s \n", k.c_str());
        lru.put(k, v, fixed);
        return 0;
    }

    void setfixed(const std::string &k, bool isfixed){
        lru.setfixed(k, isfixed);
    }

    int lookup(const std::string &k, cached* &v){
       return lru.get(k, v); 
    }

    int evict(const std::string &k){
        cached *v;
        if(lru.del(k, v)==0){
            fprintf(stderr, "LRU evict cache %s \n", k.c_str());
            return 0;
        }
        return -1;
    }
};

#endif
