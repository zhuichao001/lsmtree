#include "sstable.h"

sstable::sstable(const int lev) {
    level = lev;
}

int sstable::create(const char *path){
    this->path = path;
    fd = ::open(path, O_RDWR | O_CREAT , 0664);
    if(fd<0) {
        fprintf(stderr, "open file error: %s\n", strerror(errno));
        ::close(fd);
        return -1;
    }
    ::ftruncate(fd, SST_LIMIT);
    return 0;
}

int sstable::load(const char *path){
    this->path = path;
    fd = ::open(path, O_RDWR, 0664);
    if(fd<0) {
        fprintf(stderr, "open file error: %s\n", strerror(errno));
        ::close(fd);
        return -1;
    }

    idxoffset = 0;
    datoffset = SST_LIMIT;

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

int sstable::close(){
    ::close(fd);
    return 0;
}

int sstable::get(const std::string &key, std::string &val){
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

int sstable::put(const std::string &key, const std::string &val, int flag){
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

    uplimit(key);
    return 0;
}

int sstable::reset(const std::vector<kvtuple > &tuples){
    idxoffset = 0;
    datoffset = SST_LIMIT;
    for(auto it = tuples.begin(); it!=tuples.end(); ++it){
        const kvtuple &t = *it;
        put(t.ckey, t.cval, t.flag);
    }

    int meta[4] = {0,0,0,0}; // indicate ENDING
    pwrite(fd, meta, sizeof(meta), idxoffset);
    return 0;
}

int sstable::scan(std::function<int(const char*, const char*, int)> func){
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
        func(ckey, cval, flag);
    }
    return 0;
}

int sstable::peek(int idxoffset, kvtuple &record) {
    if(idxoffset & (sizeof(int)*4-1)!=0){
        return -1;
    }

    int meta[4];
    pread(fd, meta, sizeof(meta), idxoffset);
    const int code = meta[0];
    const int offset = meta[1];
    const int datlen = meta[2];
    record.flag   = meta[3];

    if(code==0 && offset==0 && datlen==0 && record.flag==0){
        return -1;
    }

    char data[datlen];
    pread(fd, data, datlen, offset);

    loadkv(data, &record.ckey, &record.cval);
    return 0;
}

int sstable::endindex(){
    return idxoffset;
}

sstable *create_sst(int level, int filenumber){
    sstable *sst = new sstable(level);

    char path[128];
    sprintf(path, "data/sst/%d/\0", level);
    if(!exist(path)){
        mkdir(path);
    }

    sprintf(path, "data/sst/%d/%09d.sst\0", level, filenumber);
    sst->create(path);
    return sst;
}
