#include <iostream>
#include "lsmtree.h"

void test0(){
    const int COUNT = 9;
    lsmtree db;
    db.open("./data");
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);
        db.put(k, v);
    }

    std::string val;
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);
        db.get(k,  val);
        if(std::string(v)==val){
            std::cout<< "yes same:" <<k <<std::endl;
        }else{
            std::cout<< "not same!" << k << ":" << val << ", expect:" << v << std::endl;
        }
    }
}

int main(){
    test0();
    return 0;
}
