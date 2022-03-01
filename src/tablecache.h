#ifndef _LSMTREE_TABLECACHE_H_
#define _LSMTREE_TABLECACHE_H_

#include "lru.h"

class cached{
public:
    virtual void cache()=0;
    virtual void uncache()=0;
};

class tablecache{
    LRUCache<std::string, cached *> lru;
public:
    tablecache():
        lru(512){ //default:512 sstables will be cached at most
    }

    void insert(const std::string &k, cached* v){
        lru.put(k, v);
        v->uncache();
        v->cache();
    }

    int lookup(const std::string &k, cached* &v){
       return lru.get(k, v); 
    }

    int evict(const std::string &k){
        cached *v;
        if(lru.get(k, v)==0){
            v->uncache();
            return 0;
        }
        return -1;
    }
};

#endif
