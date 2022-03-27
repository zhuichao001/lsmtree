#include <iostream>
#include "lsmtree.h"
#include "clock.h"

const int COUNT = 10000000;

void test_w(lsmtree &db){
    woptions wopt;
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);

        long start = get_time_usec();
        db.put(wopt, k, v);
        long cost = get_time_usec() - start;
        std::cout << "cost:"<<cost <<" insert " << k << ", hashcode:" << hash(k, strlen(k)) <<std::endl;
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
        long start = get_time_usec();
        db.get(ropt, k, val);
        long end = get_time_usec();
        long cost = end-start;
        if(std::string(v)==val){
            std::cerr << "cost:"<< cost << " yes same! key:" <<k <<std::endl;
        }else{
            std::cerr << "cost:"<< cost << " not same!" << k << ":" << val << ", expect:" << v << std::endl;
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
