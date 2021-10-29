#ifndef _PRIMARY_SSTABLE_H
#define _PRIMARY_SSTABLE_H

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
#include "types.h"

const int MEM_FILE_LIMIT = 20<<20; //20M 

class primarysst{
    std::string path;
    int fd;
    char *mem;
    int idxoffset;
    int datoffset;
    std::multimap<int, kvtuple> codemap; //code->idxoffset

    int restoremeta(){
        idxoffset = 0;
        datoffset = MEM_FILE_LIMIT;
        for(int pos=0; pos<MEM_FILE_LIMIT; pos+=sizeof(int)*4){
            int hashcode = *(int*)(mem+pos);
            int offset = *(int*)(mem+pos+sizeof(int));
            int datalen = *(int*)(mem+pos+sizeof(int)*2);
            int flag = *(int*)(mem+pos+sizeof(int)*3);

            idxoffset = pos;
            if(hashcode==0 && offset==0){
                break;
            }
            datoffset = offset;
            char *ckey, *cval;
            loadkv(mem+datoffset, &ckey, &cval);
            codemap.insert(std::make_pair(hashcode, kvtuple{ckey, cval, flag}));
        }
        return 0;
    }

public:
    primarysst():
        fd(-1),
        mem(nullptr),
        idxoffset(8),
        datoffset(MEM_FILE_LIMIT){
    }

    ~primarysst(){
        ::munmap(mem, MEM_FILE_LIMIT);
    }

    int create(const char *path){
        this->path = path;
        fd = ::open(path, O_RDWR | O_CREAT , 0664);
        if(fd<0) {
            fprintf(stderr, "open file error: %s\n", strerror(errno));
            ::close(fd);
            return -1;
        }
        ::ftruncate(fd, MEM_FILE_LIMIT);

        mem = (char*)::mmap(nullptr, MEM_FILE_LIMIT, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
        if(mem == MAP_FAILED) {
            fprintf(stderr, "mmap error: %s\n", strerror(errno));
            ::close(fd);
            return -1;
        }

        ::close(fd);
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

        struct stat sb;
        if (stat(path, &sb) < 0) {
            fprintf(stderr, "stat %s fail\n", path);
            ::close(fd);
            return -1;
        }

        if(sb.st_size > MEM_FILE_LIMIT) {
            fprintf(stderr, "length:%d is too large\n", sb.st_size);
            ::close(fd);
            return -1;
        }

        mem = (char *)mmap(NULL, MEM_FILE_LIMIT, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (mem == (char *)-1) {
            fprintf(stderr, "mmap fail\n");
            ::close(fd);
            return -1;
        }
        ::close(fd);

        restoremeta();
        return 0;
    }

    int close(){
        return 0;
    }

    int put(const std::string &key, const std::string &val, int flag=0){
        const int keylen = key.size()+1;
        const int vallen = val.size()+1;
        const int datalen = sizeof(int) + keylen + sizeof(int) + vallen;
        if(datoffset - idxoffset <= datalen){
            return ERROR_SPACE_NOT_ENOUGH;
        }

        datoffset = datoffset - datalen;
        memcpy(mem+datoffset, &keylen, sizeof(int));
        memcpy(mem+datoffset+sizeof(int), key.c_str(), sizeof(int));
        memcpy(mem+datoffset+sizeof(int)+key.size(), &vallen, sizeof(int));
        memcpy(mem+datoffset+sizeof(int)+key.size()+sizeof(int), val.c_str(), sizeof(int));
        msync(mem+datoffset, datalen, MS_SYNC);

        const int hashcode = hash(key.c_str(), key.size());
        memcpy(mem+idxoffset, &hashcode, sizeof(int));
        memcpy(mem+idxoffset+sizeof(int), &datoffset, sizeof(int));
        memcpy(mem+idxoffset+sizeof(int)*2, &datalen, sizeof(int));
        memcpy(mem+idxoffset+sizeof(int)*3, &flag, sizeof(int));
        msync(mem+idxoffset, sizeof(int)*4, MS_SYNC);
        idxoffset += sizeof(int)*4;

        return 0;
    }

    int get(const std::string &key, std::string &val){
        const int hashcode = hash(key.c_str(), key.size());
        auto pr = codemap.equal_range(hashcode);
        if(pr.first == std::end(codemap)) {
            return -1;
        }

        for (auto iter = pr.first ; iter != pr.second; ++iter){
            const kvtuple &t = iter->second;
            if(strcmp(key.c_str(), t.k)==0){
                return t.flag==0? SUCCESS : ERROR_KEY_NOTEXIST;
            }
        }
        return -1;
    }

    int scan(std::function<int(const char*, const char*, int)> func){
        for(int pos=0; pos<idxoffset; pos+=sizeof(int)*4){
            const int offset = *(int*)(mem+pos+sizeof(int));
            const int datalen = *(int*)(mem+pos+sizeof(int)*2);
            const int flag = *(int*)(mem+pos+sizeof(int)*3);

            char *ckey, *cval;
            loadkv(mem+offset, &ckey, &cval);
            func(ckey, cval, flag);
        }
        return 0;
    }

    int release(){
        ::close(fd);
        return 0;
    }

    int remove(){
        ::close(fd);
        ::remove(path.c_str());
        return 0;
    }

};

#endif
