#ifndef _SSTABLE_H
#define _SSTABLE_H

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <map>
#include "skiplist.h"
#include "error.h"
#include "hash.h"
#include "fio.h"
#include "encode.h"

class sstable{
    std::string path;
    int fd;
    const int  FILE_LIMIT;

    int idxoffset;
    int datoffset;
    std::multimap<int, int> codemap; //code->datoffset
public:
    sstable(const int tierno):
        FILE_LIMIT((40<<20)*10*tierno){
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

    int reload(){
        idxoffset = 0;
        datoffset = FILE_LIMIT;
        int data[4];
        for(int pos=0; ; pos+=sizeof(int)*2){
            pread(fd, data, sizeof(data)*sizeof(int), pos);
            const int code = data[0];
            const int offset = data[1];
            const int len = data[2];
            const int flag = data[3]; //TODO look del_flag
            idxoffset = pos;
            if(code==0 && offset==0){
                break;
            }
            datoffset = offset;
        }
        return 0;
    }

    int close(){
        return 0;
    }

    int get(const std::string &key, std::string &val){
        int hashcode = hash(key.c_str(), key.size());
        int data[4];
        for(int pos=0; pos<idxoffset; pos+=sizeof(int)*2){
            pread(fd, data, sizeof(data)*sizeof(int), pos);
            if(hashcode==data[0]){
                const int offset = data[1];
                const int len = data[2];
                const int flag = data[3]; //TODO look del_flag
                char raw[len];
                pread(fd, raw, len, offset);
                std::string ckey;
                loadkv(raw, ckey, val);
                if(key==ckey){
                    return 0;
                }
            }
        }
        return -1;
    }

    int put(const std::string &key, const std::string &val){
        const int keylen = key.size()+1;
        const int vallen = val.size()+1;
        const int datalen = sizeof(int) + keylen + sizeof(int) + vallen;
        if(datoffset - idxoffset <= datalen){
            return ERROR_SPACE_NOT_ENOUGH;
        }

        char data[datalen];

        datoffset = datoffset - datalen;
        memcpy(data, &keylen, sizeof(int));
        memcpy(data+sizeof(int), key.c_str(), sizeof(int));
        memcpy(data+sizeof(int)+key.size(), &vallen, sizeof(int));
        memcpy(data+sizeof(int)+key.size()+sizeof(int), val.c_str(), sizeof(int));

        pwrite(fd, data, datalen, datoffset);

        const int hashcode = hash(key.c_str(), key.size());
        char index[8];
        memcpy(index, &hashcode, sizeof(int));
        memcpy(index+sizeof(int), &datoffset, sizeof(int));
        pwrite(fd, data, datalen, idxoffset);
        idxoffset += sizeof(int)*2;

        return 0;
    }

    int reset(const std::vector<std::pair<const char*, const char*> > &tuples){
        idxoffset = 0;
        datoffset = FILE_LIMIT;
        for(auto it = tuples.begin(); it!=tuples.end(); ++it){
            put(std::string((*it).first), std::string((*it).second));
        }

        int data[4] = {0,0,0,0}; // indicate ENDING
        pwrite(fd, data, sizeof(data)*sizeof(int), idxoffset);
        return 0;
    }

    int scan(std::function<int(const char*, const char*)> func){
        int data[4];
        for(int pos=0; pos<idxoffset; pos+=sizeof(int)*2){
            pread(fd, data, sizeof(data)*sizeof(int), pos);
            const int offset = data[1];
            const int len = data[2];
            const int flag = data[3]; //TODO look del_flag
            char raw[len];
            pread(fd, raw, len, offset);
            std::string key, val;
            loadkv(raw, key, val);
            func(key.c_str(), val.c_str());
        }
        return 0;
    }
};

#endif
