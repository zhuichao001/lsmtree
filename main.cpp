#include <iostream>
#include "lsmtree.h"

void test0(){
    lsmtree db;
    db.open("./data");
    for(int i=0; i<100000; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d\0", i);
        sprintf(v, "val_%d\0", i);
        db.put(k, v);
    }

    std::string val;
    for(int i=0; i<100000; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d\0", i);
        sprintf(v, "val_%d\0", i);
        db.get(k,  val);
        if(std::string(k)==val){
            std::cout<< "yes same:" <<k <<std::endl;
        }else{
            std::cout<< "not same:" <<k <<std::endl;
        }
    }
}

int main(){
    test0();
    return 0;
}
