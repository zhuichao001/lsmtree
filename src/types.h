#ifndef _LSMTREE_TYPES_H_
#define _LSMTREE_TYPES_H_

#include <string>


typedef struct kvtuple{
    uint64_t seqno;
    char *ckey;
    char *cval;
    int flag;

    /*
    kvtuple():
        seqno(0),
        ckey(nullptr),
        cval(nullptr),
        flag(0){
    }*/
}kvtuple;

#endif
