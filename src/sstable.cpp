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
            ::close(fd);
            return -1;
        }
        ::ftruncate(fd, SST_LIMIT);
        return 0;
    } else {
        fd = ::open(path, O_RDWR, 0664);
        if(fd<0) {
            fprintf(stderr, "open file error: %s\n", strerror(errno));
            ::close(fd);
            return -1;
        }
        return 0;
    }
}

int sstable::close(){
    ::close(fd);
    return 0;
}

//cached in: idxoffset, datoffset, codemap
void sstable::cache(){
    if(incache){
        return;
    }
    idxoffset = 0;
    datoffset = SST_LIMIT;
    rowmeta meta;
    for(int pos=0; ;pos+=sizeof(meta)){
        pread(fd, (void*)&meta, sizeof(meta), pos);
        idxoffset = pos;
        if(meta.hashcode==0 && meta.datoffset==0){
            break;
        }
        codemap.insert(std::make_pair(meta.hashcode, meta.datoffset));
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
    codemap.~multimap();
    incache = false;
}

int sstable::get(const uint64_t seqno, const std::string &key, std::string &val){
    const int hashcode = hash(key.c_str(), key.size());
    rowmeta meta;
    //TODO optimize 
    for(int pos=0; pos<idxoffset; pos+=sizeof(meta)){
        pread(fd, &meta, sizeof(meta), pos);
        if(hashcode==meta.hashcode){
            if(meta.seqno>seqno){
                //fprintf(stderr, "same hashcode, but seqno too big %d, param seqno:%d\n", meta.seqno, seqno);
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
                //fprintf(stderr, "success found, %s:%s %d, seqno:%d\n", ckey, cval, meta.seqno, seqno);
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

    if(datoffset - idxoffset <= datlen+sizeof(rowmeta)){
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

    rowmeta meta = {0, 0,0,0,0}; // indicate ENDING
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
    pread(fd, &meta, sizeof(meta), idxoffset);

    if(meta.hashcode==0 && meta.datoffset==0 && meta.datlen==0 && meta.flag==0){
        return -1;
    }

    record.reserve(meta.datlen);
    pread(fd, (void*)record.data(), meta.datlen, meta.datoffset);

    loadkv(record.data(), &record.ckey, &record.cval);
    record.seqno = meta.seqno;
    record.flag = meta.flag;
    return 0;
}

int sstable::endindex(){
    return idxoffset;
}
