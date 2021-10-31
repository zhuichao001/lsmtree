#ifndef _SSTABLE_H
#define _SSTABLE_H

#include <map>
#include <atomic>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include "skiplist.h"
#include "error.h"
#include "hash.h"
#include "fio.h"
#include "encode.h"
#include "types.h"

class sstable{
    std::string path;
    int fd;
    const int  FILE_LIMIT;
    const int level;

    std::atomic<int> idxoffset;
    std::atomic<int> datoffset;
    std::multimap<int, int> codemap; //hashcode => datoffset
public:
    sstable(const int lev):
        level(lev),
        FILE_LIMIT((4<<20)*pow(8, level)){
        idxoffset = 0;
        datoffset = FILE_LIMIT;
    }

    int create(const char *path){
        this->path = path;
        fd = ::open(path, O_RDWR | O_CREAT , 0664);
        if(fd<0) {
            fprintf(stderr, "open file error: %s\n", strerror(errno));
            ::close(fd);
            return -1;
        }
        ::ftruncate(fd, FILE_LIMIT);
        return 0;
    }

    int load(const char *path){
        this->path = path;
        fd = ::open(path, O_RDWR, 0664);
        if(fd<0) {
            fprintf(stderr, "open file error: %s\n", strerror(errno));
            ::close(fd);
            return -1;
        }

        idxoffset = 0;
        datoffset = FILE_LIMIT;

        int meta[4];
        for(int pos=0; ; pos+=sizeof(meta)){
            pread(fd, meta, sizeof(meta), pos);
            const int code   = meta[0];
            const int offset = meta[1];
            const int len    = meta[2];
            const int flag   = meta[3]; //TODO look del_flag
            idxoffset = pos;
            if(code==0 && offset==0){
                break;
            }
            datoffset = offset;
        }
        return 0;
    }

    int close(){
        ::close(fd);
        return 0;
    }

    int get(const std::string &key, std::string &val){
        const int hashcode = hash(key.c_str(), key.size());
        int meta[4];
        for(int pos=0; pos<idxoffset; pos+=sizeof(meta)){
            pread(fd, meta, sizeof(meta), pos);
            if(hashcode==meta[0]){
                const int offset = meta[1];
                const int datlen = meta[2];
                const int flag = meta[3];
                if(flag==FLAG_DEL){
                    continue;
                }

                char data[datlen];
                pread(fd, data, datlen, offset);
                char *ckey, *cval;
                loadkv(data, &ckey, &cval);
                if(strcmp(ckey, key.c_str())==0){
                    val.assign(cval);
                    return 0;
                }
            }
        }
        return ERROR_KEY_NOTEXIST;
    }

    int put(const std::string &key, const std::string &val, int flag=0){
        const int keylen = key.size()+1;
        const int vallen = val.size()+1;
        const int datlen = sizeof(int) + keylen + sizeof(int) + vallen;
        if(datoffset - idxoffset <= datlen+sizeof(int)*8){
            return ERROR_SPACE_NOT_ENOUGH;
        }

        char data[datlen];
        memcpy(data, &keylen, sizeof(int));
        memcpy(data+sizeof(int), key.c_str(), keylen);
        memcpy(data+sizeof(int)+keylen, &vallen, sizeof(int));
        memcpy(data+sizeof(int)+keylen+sizeof(int), val.c_str(), vallen);

        datoffset -= datlen;
        pwrite(fd, data, datlen, datoffset);

        const int hashcode = hash(key.c_str(), key.size());
        int meta[4] = {hashcode, datoffset, datlen, flag};
        pwrite(fd, meta, sizeof(meta), idxoffset);
        idxoffset += sizeof(meta);

        return 0;
    }

    int reset(const std::vector<kvtuple > &tuples){
        idxoffset = 0;
        datoffset = FILE_LIMIT;
        for(auto it = tuples.begin(); it!=tuples.end(); ++it){
            const kvtuple &t = *it;
            //fprintf(stderr, "reset key:%s, val:%s to sst level:%d \n", t.k, t.v, level);
            put(t.key, t.val, t.flag);
        }

        int meta[4] = {0,0,0,0}; // indicate ENDING
        pwrite(fd, meta, sizeof(meta), idxoffset);
        return 0;
    }

    int scan(std::function<int(const char*, const char*, int)> func){
        int meta[4];
        for(int pos=0; pos<idxoffset; pos+=sizeof(meta)){
            pread(fd, meta, sizeof(meta), pos);
            const int offset = meta[1];
            const int datlen = meta[2];
            const int flag   = meta[3];

            char data[datlen];
            pread(fd, data, datlen, offset);

            char *ckey, *cval;
            loadkv(data, &ckey, &cval);
            //fprintf(stderr, "sst scan %s:%s\n", ckey, cval);
            func(ckey, cval, flag);
        }
        return 0;
    }
};

#endif
