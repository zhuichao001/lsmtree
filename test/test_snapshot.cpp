#include <iostream>
#include "lsmtree.h"

void test0(){
    const int COUNT = 5000;
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

    ropt.snap = db.create_snapshot();

    for(int i=COUNT; i<COUNT*2; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);

        std::cout << " insert " << k << ", hashcode:" << hash(k, strlen(k)) <<std::endl;
        db.put(wopt, k, v);
    }

    std::cout << " =========insert done !!!"<<std::endl;
    sleep(2);

    std::string val;
    for(int i=COUNT-10; i<COUNT+10; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);

        std::cout << " query " << k << ", hashcode:" << hash(k, strlen(k)) <<std::endl;
        db.get(ropt, k, val);
        if(std::string(v)==val){
            std::cerr<< "yes same:" <<k <<std::endl;
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
