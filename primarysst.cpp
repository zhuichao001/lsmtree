#include "primarysst.h"


int primarysst::restoremeta(){
    idxoffset = 0;
    datoffset = SST_LIMIT;
    for(int pos=0; pos<SST_LIMIT; pos+=sizeof(int)*4){
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
    return 0;
}

int primarysst::put(const std::string &key, const std::string &val, int flag){
    const int keylen = key.size()+1;
    const int vallen = val.size()+1;
    const int datalen = sizeof(int) + keylen + sizeof(int) + vallen;
    if(datoffset - idxoffset <= datalen+sizeof(int)*8){
        return ERROR_SPACE_NOT_ENOUGH;
    }

    datoffset = datoffset - datalen;
    memcpy(mem+datoffset, &keylen, sizeof(int));
    memcpy(mem+datoffset+sizeof(int), key.c_str(), keylen);
    memcpy(mem+datoffset+sizeof(int)+keylen, &vallen, sizeof(int));
    memcpy(mem+datoffset+sizeof(int)+keylen+sizeof(int), val.c_str(), vallen);
    msync(mem+datoffset, datalen, MS_SYNC);

    const int hashcode = hash(key.c_str(), key.size());
    memcpy(mem+idxoffset, &hashcode, sizeof(int));
    memcpy(mem+idxoffset+sizeof(int), &datoffset, sizeof(int));
    memcpy(mem+idxoffset+sizeof(int)*2, &datalen, sizeof(int));
    memcpy(mem+idxoffset+sizeof(int)*3, &flag, sizeof(int));
    msync(mem+idxoffset, sizeof(int)*4, MS_SYNC);
    idxoffset += sizeof(int)*4;

    char *pkey = mem+datoffset+sizeof(int);
    char *pval = mem+datoffset+sizeof(int)+keylen+sizeof(int);
    codemap.insert(std::make_pair(hashcode, kvtuple{pkey, pval, flag}));

    uplimit(key);
    return 0;
}

int primarysst::get(const std::string &key, std::string &val){
    const int hashcode = hash(key.c_str(), key.size());

    auto pr = codemap.equal_range(hashcode);
    for (auto iter = pr.first ; iter != pr.second; ++iter){
        const kvtuple &t = iter->second;
        if(key==t.ckey && t.flag==0){
            val = t.cval;
            return SUCCESS;
        }
    }
    return ERROR_KEY_NOTEXIST;
}

int primarysst::scan(std::function<int(const char*, const char*, int)> func){
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

int primarysst::remove(){
    ::remove(path.c_str());
    return 0;
}

int primarysst::peek(int idxoffset, kvtuple &record) {
    if(idxoffset & (sizeof(int)*4-1)!=0){
        return -1;
    }

    int meta[4]; //<hashcode, datoffset, datlen, flag>
    memcpy(meta, mem+idxoffset, sizeof(int)*4);

    if(meta[0]==0 && meta[1]==0 && meta[2]==0 && meta[3]==0){
        return -1;
    }

    loadkv(mem+meta[1], &record.ckey, &record.cval);
    record.flag   = meta[3];
    return 0;
}

primarysst *create_primarysst(int filenumber){
    primarysst *pri = new primarysst;

    char path[128];
    sprintf(path, "data/pri/\0");
    if(!exist(path)){
        mkdir(path);
    }

    sprintf(path, "data/pri/%09d.pri\0", filenumber);
    pri->create(path);
    return pri;
}
