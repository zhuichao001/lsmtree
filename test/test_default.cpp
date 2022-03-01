#include <iostream>
#include "lsmtree.h"

void test0(){
    const int COUNT = 500000;
    lsmtree db;
    woptions wopt;
    roptions ropt;
    options opt;
    db.open(&opt, "./data");
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);

        std::cout << " insert " << k << ", hashcode:" << hash(k, strlen(k)) <<std::endl;
        db.put(wopt, k, v);
    }

    std::cout << " =========insert done !!!"<<std::endl;

    std::string val;
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);

        std::cout << " query " << k << ", hashcode:" << hash(k, strlen(k)) <<std::endl;
        db.get(ropt, k, val);
        if(std::string(v)==val){
            std::cerr<< "yes same! key:" <<k <<std::endl;
        }else{
            std::cerr<< "not same!" << k << ":" << val << ", expect:" << v << std::endl;
        }
        val = "";
    }
}

int main(){
    test0();
    return 0;
}
