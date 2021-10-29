#include "encode.h"

int loadkv(char *data, char **ckey, char **cval){
    const int keylen = *(int*)(data);
    *ckey = data+sizeof(int);
    *cval = data+sizeof(int)+keylen+sizeof(int);
    return 0;
}
