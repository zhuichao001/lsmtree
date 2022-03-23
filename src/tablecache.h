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
        lru(960){ //default:512 sstables will be cached at most
    }

    int insert(const std::string &k, cached* v){
        if(lru.exist(k)){
            fprintf(stderr, "warning, ignore cache %s exist\n", k.c_str());
            return -1;
        }
        lru.put(k, v);
        return 0;
    }

    int lookup(const std::string &k, cached* &v){
       return lru.get(k, v); 
    }

    int evict(const std::string &k){
        cached *v;
        if(lru.del(k, v)==0){
            return 0;
        }
        return -1;
    }
};

#endif
