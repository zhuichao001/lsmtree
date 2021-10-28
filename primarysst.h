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

const int MEM_FILE_LIMIT = 40<<20; //40M 

class primarysst{
    std::string path;
    int fd;
    char *mem;
    int idxoffset;
    int datoffset;
    std::multimap<int, int> codemap; //code->datoffset

    int restoremeta(){
        idxoffset = 0;
        datoffset = MEM_FILE_LIMIT;
        for(int pos=0; pos<MEM_FILE_LIMIT; pos+=sizeof(int)*2){
            int hashcode = *(int*)(mem+pos);
            int offset = *(int*)(mem+pos+sizeof(int));
            idxoffset = pos;
            if(hashcode==0 && offset==0){
                break;
            }
            datoffset = offset;
            codemap.insert( std::make_pair(hashcode, datoffset) );
        }
        return 0;
    }

    int loadkv(int offset, std::string &key, std::string &val){
        if(offset<=0 || offset>=MEM_FILE_LIMIT){
            return -1;
        }
        int keylen = *(int*)(mem+offset);
        key.assign(mem+offset+sizeof(int), keylen);
        int vallen = *(int*)(mem+offset+sizeof(int)+keylen);
        val.assign(mem+offset+sizeof(int)+keylen+sizeof(int), vallen);
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

    int put(const std::string &key, const std::string &val){
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
        msync(mem+datoffset, datalen, MS_SYNC);
        idxoffset += sizeof(int)*2;

        return 0;
    }

    int get(const std::string &key, std::string &val){
        const int hashcode = hash(key.c_str(), key.size());
        auto pr = codemap.equal_range(hashcode);
        if(pr.first == std::end(codemap)) {
            return -1;
        }

        for (auto iter = pr.first ; iter != pr.second; ++iter){
            std::string destkey;
            const int offset = iter->second;
            loadkv(offset, destkey, val);
            if(destkey==key){
                return 0;
            }
        }
        return -1;
    }

    int scan(std::function<int(const char*, const char*)> func){
        return 0;
    }

    int release(){
        ::close(fd);
        return 0;
    }

    int remove(){
        ::close(fd);
        return ::remove(path.c_str());
    }

};

#endif
