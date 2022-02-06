#ifndef _MEMTABLE_H
#define _MEMTABLE_H

#include <functional>
#include <string.h>
#include "skiplist.h"

const int SKIPLIST_MAX_HEIGHT = 10;
const int BRANCH_SIZE = 16;

class memtable{
    skiplist table_;
    int size_;
public:
    memtable():
        table_(SKIPLIST_MAX_HEIGHT, BRANCH_SIZE),
        size_(0){
    }

    int get(const uint64_t seqno, const std::string &key, std::string &val){
        node *cur = table_.search(key);
        while(cur!=nullptr){
            if(cur->key!=key){
                return -1;
            }
            if(cur->sequence_number>seqno){
                cur = cur->forwards[0];
            }else{
                val = cur->val;
                return 0;
            }
        }
        return -1;
    }

    int put(const uint64_t seqno, const std::string &key, const std::string &val, const uint8_t flag=0){
        size_ += key.size() + val.size() + sizeof(int)*4;
        node *neo = table_.insert(key, val, seqno, flag);
        assert(neo!=nullptr);
        //neo->print();
        return 0;
    }

    int del(const uint64_t seqno, const std::string &key){
        return put(key, "", seqno, FLAG_DEL);
    }

    int size(){
        return size_;
    }

    void clear(){
        table_.clear();
    }

    int scan(const uint64_t seqno, std::function<int(const std::string, const std::string, int)> visit){
        for(skiplist::iterator it = table_.begin(); it!=table_.end(); ++it){
            node * p = *it;
            visit(p->key, p->val, seqno, p->flag);
        }
        return 0;
    }

};

#endif
