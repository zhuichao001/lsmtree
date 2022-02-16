#ifndef _BASETABLE_H_
#define _BASETABLE_H_

#include <string.h>
#include <assert.h>
#include <functional>
#include <string>
#include "types.h"

const int MAX_LEVELS = 8;
const int SST_LIMIT = 2<<10; //default sst size:2MB
const int MAX_ALLOWED_SEEKS = SST_LIMIT / 2; //20480;  //max seeks before compaction

typedef struct{
    uint64_t seqno;
    int hashcode;
    int datoffset;
    int datlen;
    int flag;
}rowmeta;

class basetable{
public:
    int level;
    std::string path;

    int idxoffset;
    int datoffset;

    enum{NORMAL, COMPACTING};
    int state;

    int refnum;
    int allowed_seeks;
    int file_number;
    int file_size;

    std::string smallest;
    std::string largest;

    basetable():
        level(0),
        idxoffset(0),
        datoffset(SST_LIMIT),
        state(NORMAL),
        refnum(0),
        allowed_seeks(MAX_ALLOWED_SEEKS),
        smallest(64, '\xff'),
        largest(""){
    }

    void setlevel(int d){
        level = d;
    }

    int getlevel(){
        return level;
    }

    int remove(){
        ::remove(path.c_str());
        return 0;
    }

    bool overlap(basetable *other){
        return overlap(other->smallest, other->largest);
    }

    bool overlap(const std::string &start, const std::string &end){
        const std::string &lower = smallest > start? smallest: start;
        const std::string &upper = largest < end? largest: end;
        return lower <= upper;
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

    int filesize(){
        return file_size;
    }

    void uplimit(const std::string &key){
        if(key<smallest){
            smallest = key;
        }
        if(key>largest){
            largest = key;
        }
    }
public:
    virtual int create(const char *path) = 0;
    virtual int load(const char *path) = 0;
    virtual int close() = 0;

    virtual int get(const uint64_t seqno, const std::string &key, std::string &val) = 0;
    virtual int put(const uint64_t seqno, const std::string &key, const std::string &val, int flag=0) = 0;
    virtual int scan(const uint64_t seqno, std::function<int(const char* /*key*/, const char* /*val*/, int /*flag*/)> func) = 0;
    virtual int peek(int idxoffset, kvtuple &record) = 0;

    class iterator{
        basetable *table;
        int idxoffset;
        friend class basetable;
    public:
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
        //load from disk before compare
        kvtuple kva = *a; 
        kvtuple kvb = *b;
        return strcmp(kva.ckey, kvb.ckey) <=0;
    }
};

#endif
