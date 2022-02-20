#ifndef _LSMTREE_TYPES_H_
#define _LSMTREE_TYPES_H_

#include <string>


class kvtuple{
    char *buffer;
public:
    uint64_t seqno;
    char *ckey;
    char *cval;
    int flag;

    kvtuple():
        seqno(0),
        ckey(nullptr),
        cval(nullptr),
        flag(0),
        buffer(nullptr){
    }

    kvtuple(uint64_t seq, char *k, char *v, int f):
        seqno(seq),
        ckey(k),
        cval(v),
        flag(f),
        buffer(nullptr) {
    }

    void reserve(const int size){
        buffer = new char[size];
    }

    char *data(){
        return buffer;
    }

    ~kvtuple(){
        if(buffer!=nullptr){
            delete buffer;
        }
    }
};

#endif
