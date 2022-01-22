#ifndef _MEMTABLE_H
#define _MEMTABLE_H

#include <functional>
#include <string.h>
#include "skiplist.h"

class memtable{
    skiplist table_;
    int size_;
public:
    memtable():
        table_(10, 16),
        size_(0){
    }

    int get(const std::string &key, std::string &val){
        node *res = table_.search(key);
        if(res==nullptr){
            return -1;
        }
        val = res->val;
        return 0;
    }

    int put(const std::string &key, const std::string &val, const int flag=0){
        size_ += key.size() + val.size() + sizeof(int)*4;
        node *neo = table_.insert(key, val, flag);
        assert(neo!=nullptr);
        //neo->print();
        return 0;
    }

    int del(const std::string &key){
        return put(key, "", FLAG_DEL);
    }

    int size(){
        return size_;
    }

    void clear(){
        table_.clear();
    }

    int scan(std::function<int(const std::string, const std::string, int)> visit){
        for(skiplist::iterator it = table_.begin(); it!=table_.end(); ++it){
            node * p = *it;
            visit(p->key, p->val, p->flag);
        }
        return 0;
    }

};

#endif
