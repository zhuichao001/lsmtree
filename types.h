#ifndef _LSMTREE_TYPES_H_
#define _LSMTREE_TYPES_H_

class kvtuple{
public:
    kvtuple(const char *key, const char *val, int f):
        key(key),
        val(val),
        flag(f){
    }

    kvtuple(const std::string &key, const std::string &val, int f):
        key(key),
        val(val),
        flag(f){
    }

    kvtuple(const kvtuple &t):
        key(t.key),
        val(t.val),
        flag(t.flag){
    }

    kvtuple & operator=(const kvtuple &t){
        key = t.key;
        val = t.val;
        flag = t.flag;
        return *this;
    }

    std::string key;
    std::string val;
    int flag;
};

#endif
