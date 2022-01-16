#include "hash.h"

int hash(const char* src, const int len){
    static HashBase hashcall;
    return hashcall(src, len);
}
