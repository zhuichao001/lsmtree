#include "sstable.h"

extern std::string basedir;

sstable::sstable(const int lev, const int fileno, const char*leftkey, const char *rightkey):
    basetable(){
    level = lev;
    file_number = fileno;
    sprintf(path, "%s/sst/%d/%09d.sst\0", basedir.c_str(), lev, fileno);

    if(leftkey!=nullptr){
        smallest = leftkey;
    }
    if(rightkey!=nullptr){
        largest = rightkey;
    }
}

int sstable::open(){
    if(!exist(path)){
        fd = ::open(path, O_RDWR | O_CREAT , 0664);
        if(fd<0) {
            fprintf(stderr, "open file error: %s\n", strerror(errno));
            return -1;
        }
        ::ftruncate(fd, SST_LIMIT);
        return 0;
    } else {
        fd = ::open(path, O_RDWR, 0664);
        if(fd<0) {
            fprintf(stderr, "open file error: %s\n", strerror(errno));
            return -1;
        }
        return 0;
    }
}

int sstable::close(){
    if(fd>0){
        ::close(fd);
    }
    fd = -1;
    return 0;
}

//cached in: idxoffset, datoffset, codemap
void sstable::cache(){
    if(incache){
        return;
    }
    if(fd<0){
        open();
    }
    idxoffset = 0;
    datoffset = SST_LIMIT;
    rowmeta meta;
    //loop break if meta is {0,0,0,0,0}
    for(int pos=0; meta.datoffset!=0; pos+=sizeof(meta)){
        if(pread(fd, (void*)&meta, sizeof(meta), pos) < 0){
            fprintf(stderr, "cache::pread fd:%d pos:%d\n", fd, pos);
            perror("pread error::");
            return;
        }
        if(meta.seqno==0 && meta.hashcode && meta.datoffset){
            break;
        }
        idxoffset = pos;
        codemap.insert(std::make_pair(meta.hashcode, meta));
        datoffset = meta.datoffset;
    }
    incache = true; 
}

void sstable::uncache(){
    if(!incache){
        return;
    }
    idxoffset = 0;
    datoffset = SST_LIMIT;
    std::multimap<int, rowmeta> _;
    _.swap(codemap);
    incache = false;
    this->close();
}

int sstable::get(const uint64_t seqno, const std::string &key, std::string &val){
    const int hashcode = hash(key.c_str(), key.size());
    auto res = codemap.equal_range(hashcode);
    for(auto iter = res.first; iter!=res.second; ++iter){
        const rowmeta &meta = iter->second;
        if(meta.seqno>seqno){
            continue;
        }
        char data[meta.datlen];
        pread(fd, data, meta.datlen, meta.datoffset);
        char *ckey, *cval;
        loadkv(data, &ckey, &cval);
        if(strcmp(ckey, key.c_str())==0){
            if(meta.flag==FLAG_DEL){
                val.assign("");
                return ERROR_KEY_DELETED;
            }else{
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

    if(datoffset - idxoffset <= datlen+sizeof(rowmeta)*2){
        return ERROR_SPACE_NOT_ENOUGH;
    }

    char data[datlen];
    memcpy(data, &keylen, sizeof(int));
    memcpy(data+sizeof(int), key.c_str(), keylen);
    memcpy(data+sizeof(int)+keylen, &vallen, sizeof(int));
    memcpy(data+sizeof(int)+keylen+sizeof(int), val.c_str(), vallen);

    datoffset -= datlen;
    if(pwrite(fd, data, datlen, datoffset)<0){
        perror("put pwrite:");
        return -1;
    }

    const int hashcode = hash(key.c_str(), key.size());
    rowmeta meta = {seqno, hashcode, datoffset, datlen, flag};
    pwrite(fd, (void*)&meta, sizeof(rowmeta), idxoffset);
    idxoffset += sizeof(rowmeta);

    const int rowlen = sizeof(int)+keylen+sizeof(int)+vallen + sizeof(meta);
    file_size += rowlen;
    uplimit(key);
    return 0;
}

int sstable::reset(const std::vector<kvtuple > &tuples){
    idxoffset = 0;
    datoffset = SST_LIMIT;
    for(auto it = tuples.begin(); it!=tuples.end(); ++it){
        const kvtuple &t = *it;
        put(t.seqno, t.ckey, t.cval, t.flag);
    }

    rowmeta meta = {0, 0, 0, 0, 0}; // indicate ENDING
    pwrite(fd, &meta, sizeof(meta), idxoffset);
    return 0;
}

int sstable::scan(const uint64_t seqno, std::function<int(const int, const char*, const char*, int)> func){
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
        func(meta.seqno, ckey, cval, meta.flag);
    }
    return 0;
}

int sstable::peek(int idxoffset, kvtuple &record) {
    rowmeta meta;
    if(pread(fd, &meta, sizeof(meta), idxoffset)<0){
        perror("sstable::peek idxoffset");
        return -1;
    }

    if(meta.hashcode==0 && meta.datoffset==0 && meta.datlen==0 && meta.flag==0){
        return -1;
    }

    assert(meta.datlen>0);
    record.reserve(meta.datlen);
    if(pread(fd, (void*)record.data(), meta.datlen, meta.datoffset)<0){
        perror("sstable::peek kvtuple");
        return -1;
    }

    loadkv(record.data(), &record.ckey, &record.cval);
    record.seqno = meta.seqno;
    record.flag = meta.flag;
    return 0;
}

int sstable::endindex(){
    return idxoffset;
}
