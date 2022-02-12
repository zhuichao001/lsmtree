#include "types.h"
#include "primarysst.h"


int primarysst::restoremeta(){
    idxoffset = 0;
    datoffset = SST_LIMIT;
    for(int pos=0; pos<SST_LIMIT; pos+=sizeof(rowmeta)){
        const rowmeta meta = *(rowmeta*)(mem+pos);

        idxoffset = pos;
        if(meta.hashcode==0 && meta.datoffset==0){
            break;
        }
        datoffset = meta.datoffset;
        char *ckey, *cval;
        loadkv(mem+datoffset, &ckey, &cval);
        kvtuple t = {meta.seqno, ckey, cval, meta.flag};
        codemap.insert(std::make_pair(meta.hashcode, t));
    }
    return 0;
}

primarysst::primarysst():
    mem(nullptr) {
    level = 0;
}

primarysst::~primarysst(){
    ::munmap(mem, SST_LIMIT);
}

int primarysst::create(const char *path){
    this->path = path;
    int fd = ::open(path, O_RDWR | O_CREAT , 0664);
    if(fd<0) {
        fprintf(stderr, "open file error: %s\n", strerror(errno));
        ::close(fd);
        return -1;
    }
    ::ftruncate(fd, SST_LIMIT);

    mem = (char*)::mmap(nullptr, SST_LIMIT, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if(mem == MAP_FAILED) {
        fprintf(stderr, "mmap error: %s\n", strerror(errno));
        ::close(fd);
        return -1;
    }

    ::close(fd);
    return 0;
}

int primarysst::load(const char *path){
    this->path = path;
    int fd = ::open(path, O_RDWR, 0664);
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

    if(sb.st_size > SST_LIMIT) {
        fprintf(stderr, "length:%d is too large\n", sb.st_size);
        ::close(fd);
        return -1;
    }

    mem = (char *)mmap(NULL, SST_LIMIT, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mem == (char *)-1) {
        fprintf(stderr, "mmap fail\n");
        ::close(fd);
        return -1;
    }
    ::close(fd);

    restoremeta();
    return 0;
}

int primarysst::close(){
    munmap(mem, SST_LIMIT);
    mem = nullptr;
    return 0;
}

int primarysst::put(const uint64_t seqno, const std::string &key, const std::string &val, int flag){
    const int keylen = key.size()+1;
    const int vallen = val.size()+1;
    const int datlen = sizeof(int) + keylen + sizeof(int) + vallen;
    if(datoffset - idxoffset <= datlen+sizeof(int)*8){
        return ERROR_SPACE_NOT_ENOUGH;
    }

    datoffset = datoffset - datlen;
    memcpy(mem+datoffset, &keylen, sizeof(int));
    memcpy(mem+datoffset+sizeof(int), key.c_str(), keylen);
    memcpy(mem+datoffset+sizeof(int)+keylen, &vallen, sizeof(int));
    memcpy(mem+datoffset+sizeof(int)+keylen+sizeof(int), val.c_str(), vallen);
    msync(mem+datoffset, datlen, MS_SYNC);

    const int hashcode = hash(key.c_str(), key.size());

    rowmeta meta = {seqno, hashcode, datoffset, datlen, flag};

    memcpy(mem+idxoffset, &meta, sizeof(meta));
    msync(mem+idxoffset, sizeof(meta), MS_SYNC);
    idxoffset += sizeof(meta);

    char *pkey = mem+datoffset+sizeof(int);
    char *pval = mem+datoffset+sizeof(int)+keylen+sizeof(int);
    codemap.insert(std::make_pair(hashcode, kvtuple{seqno, pkey, pval, flag}));

    uplimit(key);
    return 0;
}

int primarysst::get(const uint64_t seqno, const std::string &key, kvtuple &res){
    const int hashcode = hash(key.c_str(), key.size());

    auto pr = codemap.equal_range(hashcode);
    for (auto iter = pr.first ; iter != pr.second; ++iter){
        const kvtuple &t = iter->second;
        if(t.seqno > seqno){
            continue;
        }
        if(key==t.ckey && t.flag==FLAG_VAL){
            res = t;
            return SUCCESS;
        }
    }
    return ERROR_KEY_NOTEXIST;
}

int primarysst::get(const uint64_t seqno, const std::string &key, std::string &val){
    kvtuple res;
    int err = get(seqno, key, res);
    if(err<0){
        return err;
    }
    val = res.cval;
    return SUCCESS;
}

int primarysst::scan(const uint64_t seqno, std::function<int(const char*, const char*, int)> func){
    for(int pos=0; pos<idxoffset; pos+=sizeof(rowmeta)){
        const rowmeta meta = *(rowmeta*)(mem+pos);
        if(meta.seqno > seqno){
            continue;
        }

        char *ckey, *cval;
        loadkv(mem+meta.datoffset, &ckey, &cval);
        func(ckey, cval, meta.flag);
    }
    return 0;
}

int primarysst::peek(int idxoffset, kvtuple &record) {
    if(idxoffset & (sizeof(int)*4-1)!=0){
        return -1;
    }

    const rowmeta meta = *(rowmeta*)(mem+idxoffset);
    if(meta.seqno==0 && meta.hashcode==0){
        return -1;
    }

    loadkv(mem+meta.datoffset, &record.ckey, &record.cval);
    record.flag = meta.flag;
    return 0;
}

primarysst *create_primarysst(int filenumber){
    primarysst *pri = new primarysst;

    char path[128];
    sprintf(path, "data/pri/\0"); //TODO
    if(!exist(path)){
        mkdir(path);
    }

    sprintf(path, "data/pri/%09d.pri\0", filenumber);
    pri->create(path);
    return pri;
}