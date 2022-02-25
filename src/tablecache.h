#ifndef _LSMTREE_TABLECACHE_H_
#define _LSMTREE_TABLECACHE_H_

#include "lru.h"

class cached{
public:
    virtual void cache()=0;
    virtual void uncache()=0;
};

class tablecache{
    LRUCache<std::string, cached *> lfu;
public:
    tablecache():
        lfu(512){ //default:512 sstable cached
    }

    void insert(const std::string &k, cached* v){
        lfu.put(k, v);
        v->cache();
    }

    int lookup(const std::string &k, cached* &v){
       return lfu.get(k, v); 
    }

    int evict(const std::string &k){
        cached *v;
        if(lfu.get(k, v)==0){
            v->uncache();
            return 0;
        }
        return -1;
    }
};

#endif
