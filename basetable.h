#ifndef _BASETABLE_H_
#define _BASETABLE_H_

#include <string.h>
#include <functional>
#include <string>
#include "types.h"

const int SST_LIMIT = 2<<20; //default sst size:2MB


class basetable{
public:
    int level;
    std::string path;
    int fd;
    int idxoffset;
    int datoffset;

    enum{COMPLETE, COMPACTING};
    int state;
    std::string smallest;
    std::string biggest;

    basetable():
        level(0),
        fd(-1),
        idxoffset(0),
        datoffset(SST_LIMIT),
        state(COMPLETE),
        smallest(64, '\xff'),
        biggest(""){
    }

public:
    virtual int create(const char *path) = 0;
    virtual int load(const char *path) = 0;
    virtual int close() = 0;

    virtual int get(const std::string &key, std::string &val) = 0;
    virtual int put(const std::string &key, const std::string &val, int flag=0) = 0;
    virtual int scan(std::function<int(const char*, const char*, int)> func) = 0;
    virtual int peek(int idxoffset, kvtuple &record) = 0;

    class iterator{
    public:
        basetable *table;
        int idxoffset;

        bool valid(){
            return idxoffset < table->idxoffset;
        }

        iterator &next(){
            idxoffset += sizeof(int)*4;
            return *this;
        }

        bool operator==(iterator &other){
            return (table==other.table) && (idxoffset==other.idxoffset);
        }

        bool operator<(iterator &other){
            kvtuple kva = **this;
            kvtuple kvb = *other;
            return strcmp(kva.ckey, kvb.ckey) <0;
        }

        const kvtuple operator*() const {
            kvtuple record;
            table->peek(idxoffset, record);
            return record;
        }
    };

    iterator begin(){
        iterator it;
        it.table = this;
        it.idxoffset = 0;
        return it;
    }

    iterator end(){
        iterator it;
        it.table = this;
        it.idxoffset = this->idxoffset;
        return it;
    }

    static bool compare(const iterator &a, const iterator &b){
        kvtuple kva = *a;
        kvtuple kvb = *b;
        return strcmp(kva.ckey, kvb.ckey) <=0;
    }

    void uplimit(const std::string &key){
        if(key<smallest){
            smallest = key;
        }
        if(key>biggest){
            biggest = key;
        }
    }

    bool overlap(basetable *other){
        std::string &lower = smallest > other->smallest? smallest: other->smallest;
        std::string &upper = biggest < other->biggest? biggest: other->biggest;
        return lower <= upper;
    }
};

#endif