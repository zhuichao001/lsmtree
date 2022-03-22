#ifndef _MEMTABLE_H
#define _MEMTABLE_H

#include <string>
#include <functional>
#include <string.h>
#include "types.h"
#include "skiplist.h"

const int MAX_MEMTAB_SIZE = 2<<20; //2MB
const int SKIPLIST_MAX_HEIGHT = 10;
const int BRANCH_SIZE = 16;

typedef struct{
    int logidx;
    uint64_t seqno;
    std::string key;
    std::string val;
    uint8_t flag;
} onval;


class memtable{
    skiplist<onval *> table_;
    int size_;
    int refnum;
public:
    memtable():
        table_(SKIPLIST_MAX_HEIGHT, BRANCH_SIZE),
        size_(0),
        refnum(0){
    }

    void ref(){
        ++refnum;
    }

    void unref(){
        assert(refnum>=1);
        if(--refnum==0){
            delete this;
        }
    }

    int get(const uint64_t seqno, const std::string &key, std::string &val);

    int put(const int logidx, const uint64_t seqno, const std::string &key, const std::string &val, const uint8_t flag=FLAG_VAL);

    int del(const int logidx, const uint64_t seqno, const std::string &key);

    int size(){
        return size_;
    }

    void clear(){
        table_.clear();
    }

    int scan(const uint64_t seqno, std::function<int(int /*logidx*/, uint64_t /*seqno*/, const std::string &/*key*/, const std::string &/*val*/, int /*flag*/)> visit);

    void print(int seqno){
        this->scan(seqno, [seqno](int logidx, uint64_t seq, const std::string &key, const std::string &val, int flag) -> int{
            fprintf(stderr, "  logidx:%d seq:%d<%d key:%s val:%s flag:%d\n", logidx, seq, seqno, key.c_str(), val.c_str(), flag);
            return 0;
        });
    }

public:
    class iterator{
        skiplist<onval *> *table;
        node<onval *> *ptr;
        friend class memtable;
    public:
        node<onval *> * operator *(){
            return ptr;
        }

        onval* val(){
            assert(ptr!=nullptr);
            return ptr->val;
        }

        iterator & operator ++(){
            ptr = ptr->next();
            return *this;
        }

        bool operator !=(const iterator &other){
            return ptr!=other.ptr;
        }

        void next(){
            ptr = ptr->next();
        }

        bool valid(){
            return ptr != table->nil;
        }

    };

    iterator begin(){
        iterator it;
        it.table = &table_;
        it.ptr = table_.head->next();
        assert(it.ptr!=nullptr);
        return it;
    }

    iterator end(){
        iterator it;
        it.table = &table_;
        it.ptr = table_.nil;
        return it;
    }

    static bool compare_gt(iterator &a, iterator &b){
        node<onval *> * na = *a;
        node<onval *> * nb = *b;
        int delta = strcmp(na->val->key.c_str(), nb->val->key.c_str());
        if(delta!=0){
            return delta > 0;
        }
        return na->val->seqno < na->val->seqno;
    }
};

#endif
