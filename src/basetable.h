#ifndef _BASETABLE_H_
#define _BASETABLE_H_

#include <string.h>
#include <assert.h>
#include <functional>
#include <string>
#include <mutex>
#include "tablecache.h"
#include "types.h"

extern std::string basedir;

const int MAX_LEVELS = 8;
const int SST_LIMIT = 1<<19; //default sst size:512KB
const int MAX_ALLOWED_SEEKS = SST_LIMIT / 64;  //max seeks before compaction

class basetable: public cached {
public:
    int level;
    char path[64];

    int idxoffset;
    int datoffset;

    int allowed_seeks;
    int file_number;
    int file_size;

    std::string smallest;
    std::string largest;

    int keynum;
    int ref_num;

    std::mutex mutex;
    bool isclosed;
    bool isloaded;
    bool incache;

    basetable():
        level(0),
        idxoffset(0),
        datoffset(SST_LIMIT),
        file_number(0),
        file_size(0),
        allowed_seeks(MAX_ALLOWED_SEEKS),
        smallest(64, '\xff'),
        largest(""),
        keynum(0),
        ref_num(0),
        isclosed(true), 
        isloaded(false),
               incache(false) {
    }

    int remove(){
        return ::remove(path);
    }

    bool overlap(const std::string &start, const std::string &end){
        const std::string &lower = smallest > start? smallest: start;
        const std::string &upper = largest < end? largest: end;
        return lower <= upper;
    }

    bool containedby(const std::string &start, const std::string &end){
        return smallest>=start && largest<=end;
    }

    int ref(){
        return ++ref_num;
    }

    int unref(){
        assert(ref_num>=1);
        int refcnt = --ref_num;
        if(refcnt==0){
            fprintf(stderr, "unref zero destroy level-%d sst-%d <%s,%s>\n", level, file_number, smallest.c_str(), largest.c_str());
            delete this;
        }
        return refcnt;
    }

    int refnum(){
        return ref_num;
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

    bool cache(){
        std::unique_lock<std::mutex> lock{mutex};
        if(isclosed){
            open();
        }
        if(!isloaded){
            fprintf(stderr, "LOAD IN CACHE %s\n", path);
            load(); //cache: idxoffset, datoffset, codemap
        }
        incache = true;
        return true;
    }

    void uncache(){
        std::unique_lock<std::mutex> lock{mutex};
        if(isloaded){
            fprintf(stderr, "UNCACHEING %s\n", path);
            release();
        }
        if(!isclosed){
            close();
        }
        incache = false;
    }

    bool iscached(){
        std::unique_lock<std::mutex> lock{mutex};
        return incache;
    }

    void printinfo(){
        fprintf(stderr, "%d %d %s %s %d\n", level, file_number, smallest.c_str(), largest.c_str(), keynum);
    }

public:
    virtual int open() = 0;
    virtual int close() = 0;
    virtual int load() = 0;
    virtual int release() = 0;

    virtual int get(const uint64_t seqno, const std::string &key, std::string &val) = 0;
    virtual int put(const uint64_t seqno, const std::string &key, const std::string &val, int flag=0) = 0;
    virtual int scan(const uint64_t seqno, std::function<int(const int /*seq*/, const char* /*key*/, const char* /*val*/, int /*flag*/)> func) = 0;
    virtual int peek(int idxoffset, kvtuple &record) = 0;

    void print(int seqno){
        this->scan(seqno, [seqno](const int seq, const char *k, const char *v, int flag) -> int{
            fprintf(stderr, "  seq:%d<%d key:%s val:%s flag:%d\n", seq, seqno, k, v, flag);
            return 0;
        });
    }

    class iterator{
        basetable *table;
        int idxoffset;
        friend class basetable;
    public:
        bool valid(){
            return idxoffset < table->idxoffset;
        }

        basetable *belong(){
            return table;
        }

        iterator &next(){
            idxoffset += sizeof(rowmeta);
            return *this;
        }

        bool operator==(iterator &other){
            return (table==other.table) && (idxoffset==other.idxoffset);
        }

        bool operator<(iterator &other){
            kvtuple kva, kvb;
            this->parse(kva);
            other.parse(kvb);
            return strcmp(kva.ckey, kvb.ckey) <0;
        }

        void parse(kvtuple &record) const {
            table->peek(idxoffset, record);
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

    static bool compare_gt(const iterator &a, const iterator &b){
        //load from disk before compare
        kvtuple kva, kvb;
        a.parse(kva);
        b.parse(kvb);
        assert(kva.ckey!=nullptr);
        assert(kvb.ckey!=nullptr);
        int delta = strcmp(kva.ckey, kvb.ckey);
        if(delta!=0){
            return delta > 0;
        }
        return kva.seqno < kvb.seqno;
    }
};

inline int loadkv(char *data, char **ckey, char **cval){
    const int keylen = *(int*)(data);
    *ckey = data+sizeof(int);
    *cval = data+sizeof(int)+keylen+sizeof(int);
    return 0;
}

#endif
