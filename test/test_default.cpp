#include <iostream>
#include "lsmtree.h"
#include "clock.h"

const int COUNT = 5000000;

void test_w(lsmtree &db){
    woptions wopt;
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);

        std::cout << " insert " << k << ", hashcode:" << hash(k, strlen(k)) <<std::endl;
        db.put(wopt, k, v);
    }
    std::cout << "======insert done ======"<<std::endl;
}

void test_r(lsmtree &db){
    roptions ropt;
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
    std::cout << "======get done ======"<<std::endl;
}

int test(){
    lsmtree db;
    options opt;
    if(db.open(&opt, "./data")<0){
        return -1;
    }

    std::cout << "WRITE BEGIN " << timestamp() << std::endl;
    test_w(db);
    std::cout << "WRITE END " << timestamp() << std::endl;

    std::cout << "READ BEGIN " << timestamp() << std::endl;
    test_r(db);
    std::cout << "READ END " << timestamp() << std::endl;

    return 0;
}

int main(){
    test();
    return 0;
}
