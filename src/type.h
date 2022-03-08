#ifndef _LSMTREE_TYPE_H
#define _LSMTREE_TYPE_H

#include <stdint.h>

//OPERATION:put/delete
enum{
    FLAG_VAL = 0,
    FLAG_DEL = 1,
};

//used for sst formation
typedef struct{
    uint64_t seqno;
    int hashcode;
    int datoffset;
    int datlen;
    int flag;
}rowmeta;

//used for row data access
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
