#ifndef _MEMTABLE_H
#define _MEMTABLE_H

#include <functional>
#include <string.h>
#include "skiplist.h"

class memtable{
    skiplist dict;
public:
    memtable():
        dict(10, 16){
    }

    int get(const std::string &key, std::string &val){
        node *res = dict.search(key);
        if(res==nullptr){
            return -1;
        }
        val = res->val;
        return 0;
    }

    int put(const std::string &key, const std::string &val, const int flag=0){
        dict.insert(key, val, flag);
        return 0;
    }

    int del(const std::string &key){
        return put(key, "", FLAG_DEL);
    }

    int size(){
        return dict.size();
    }

    int scan(std::function<int(const std::string, const std::string, int)> visit){
        for(skiplist::iterator it = dict.begin(); it!=dict.end(); ++it){
            node * p = *it;
            visit(p->key, p->val, p->flag);
        }
        return 0;
    }

};

#endif
