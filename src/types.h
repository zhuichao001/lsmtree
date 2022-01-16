#ifndef _LSMTREE_TYPES_H_
#define _LSMTREE_TYPES_H_

class kvtuple{
public:
    kvtuple():
        ckey(nullptr),
        cval(nullptr),
        flag(0){
    }

    kvtuple(char *k, char *v, int f):
        ckey(k),
        cval(v),
        flag(f){
    }

    kvtuple(const kvtuple &t):
        ckey(t.ckey),
        cval(t.cval),
        flag(t.flag){
    }

    kvtuple & operator=(const kvtuple &t){
        ckey = t.ckey;
        cval = t.cval;
        flag = t.flag;
        return *this;
    }

    char *ckey;
    char *cval;
    int flag;
};

#endif
