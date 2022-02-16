#include "sstable.h"

sstable::sstable(const int lev, const int fileno):
    basetable(){
    level = lev;
    file_number = fileno;
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

    rowmeta meta;
    for(int pos=0; ;pos+=sizeof(meta)){
        pread(fd, (void*)&meta, sizeof(meta), pos);

        idxoffset = pos;
        if(meta.hashcode==0 && meta.datoffset==0 && meta.datlen==0 && meta.flag==0){
            break;
        }
        datoffset = meta.datoffset;
    }
    return 0;
}

int sstable::close(){
    ::close(fd);
    return 0;
}

int sstable::get(const uint64_t seqno, const std::string &key, std::string &val){
    const int hashcode = hash(key.c_str(), key.size());
    rowmeta meta;
    //TODO optimize 
    for(int pos=0; pos<idxoffset; pos+=sizeof(meta)){
        pread(fd, &meta, sizeof(meta), pos);
        if(hashcode==meta.hashcode){
            if(meta.seqno>seqno){
                continue;
            }
            if(meta.flag==FLAG_DEL){
                return ERROR_KEY_DELETED;
            }

            char data[meta.datlen];
            pread(fd, data, meta.datlen, meta.datoffset);
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

int sstable::put(const uint64_t seqno, const std::string &key, const std::string &val, int flag){
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
    rowmeta meta = {seqno, hashcode, datoffset, datlen, flag};
    pwrite(fd, (void*)&meta, sizeof(meta), idxoffset);
    idxoffset += sizeof(meta);

    uplimit(key);
    return 0;
}

int sstable::reset(const std::vector<kvtuple > &tuples){
    idxoffset = 0;
    datoffset = SST_LIMIT;
    for(auto it = tuples.begin(); it!=tuples.end(); ++it){
        const kvtuple t = *it;
        put(t.seqno, t.ckey, t.cval, t.flag);
    }

    rowmeta meta = {0, 0,0,0,0}; // indicate ENDING
    pwrite(fd, &meta, sizeof(meta), idxoffset);
    return 0;
}

int sstable::scan(const uint64_t seqno, std::function<int(const char*, const char*, int)> func){
    rowmeta meta;
    for(int pos=0; pos<idxoffset; pos+=sizeof(meta)){
        pread(fd, (void*)&meta, sizeof(meta), pos);
        if(meta.seqno>seqno){
            continue;
        }

        char data[meta.datlen];
        pread(fd, data, meta.datlen, meta.datoffset);

        char *ckey, *cval;
        loadkv(data, &ckey, &cval);
        func(ckey, cval, meta.flag);
    }
    return 0;
}

int sstable::peek(int idxoffset, kvtuple &record) {
    rowmeta meta;
    pread(fd, &meta, sizeof(meta), idxoffset);

    if(meta.hashcode==0 && meta.datoffset==0 && meta.datlen==0 && meta.flag==0){
        return -1;
    }

    char data[meta.datlen];
    pread(fd, (void*)data, meta.datlen, meta.datoffset);

    loadkv(data, &record.ckey, &record.cval);
    record.flag = meta.flag;
    return 0;
}

int sstable::endindex(){
    return idxoffset;
}

sstable *create_sst(int level, int filenumber){
    sstable *sst = new sstable(level, filenumber);

    char path[128];
    sprintf(path, "data/sst/%d/\0", level);
    if(!exist(path)){
        mkdir(path);
    }

    sprintf(path, "data/sst/%d/%09d.sst\0", level, filenumber);
    sst->create(path);
    return sst;
}
