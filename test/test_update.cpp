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
        if(i%2==1){
            db.del(wopt, k);
        }
    }

    ropt.snap = db.create_snapshot();
    fprintf(stderr, "snapshot:%d\n", ropt.snap->sequence());

    for(int i=COUNT/2; i<COUNT*2; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d_%d", i, i);

        std::cout << " insert " << k << ", hashcode:" << hash(k, strlen(k)) <<std::endl;
        db.put(wopt, k, v);
    }

    std::cout << " =========insert done !!!"<<std::endl;
    sleep(2);

    for(int i=0; i<COUNT+10; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        if(i<COUNT){ 
            if(i%2==0){
                sprintf(v, "val_%d", i);
            }else{
                sprintf(v, "\0");
            }
        }else{
                sprintf(v, "\0");
        }

        std::cout << " query " << k << ", hashcode:" << hash(k, strlen(k)) <<std::endl;
        std::string val;
        db.get(ropt, k, val);
        if(val==v){
            std::cerr<< "yes same!" << k << ":" << val << ", expect:" << v << std::endl;
        }else{
            std::cerr<< "not same!" << k << ":" << val << ", expect:" << v << std::endl;
        }
    }

    roptions ropt2;
    for(int i=0; i<COUNT+10; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        if(i<COUNT/2){ 
            if(i%2==0){
                sprintf(v, "val_%d", i);
            }else{
                sprintf(v, "\0");
            }
        }else{
             sprintf(v, "val_%d_%d", i, i);
        }

        std::cout << " query " << k << ", hashcode:" << hash(k, strlen(k)) <<std::endl;
        std::string val;
        db.get(ropt2, k, val);
        if(val==v){
            std::cerr<< "yes same!" << k << ":" << val << ", expect:" << v << std::endl;
        }else{
            std::cerr<< "not same!" << k << ":" << val << ", expect:" << v << std::endl;
        }
    }
}


int main(){
    test0();
    return 0;
}
