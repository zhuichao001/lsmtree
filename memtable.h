#ifndef _MEMTABLE_H
#define _MEMTABLE_H

#include "sliplist.h"

class memtable{
    skiplist dict;
public:
    int get(const std::string &key, std::string &val){
        node *res = dict.search(key);
        if(res==nullptr){
            return -1;
        }
        val = res->val;
    }

    int put(const std::string &key, const std::string &val){
        node *res = dict.search(key);
        if(res==nullptr){
            dict.insert(key, val);
            return 1;
        }else{
            node->val = val;
        }
    }

    int del(const std::string &key){
        return put(key, "");
    }

    int size(){
        return dict.size();
    }
};

#endif
