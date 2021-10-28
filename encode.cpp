#include "encode.h"

int loadkv(char *data, std::string &key, std::string &val){
    const int keylen = *(int*)(data);
    key.assign(data+sizeof(int), keylen);
    const int vallen = *(int*)(data+sizeof(int)+keylen);
    val.assign(data+sizeof(int)+keylen+sizeof(int), vallen);
    return 0;
}

