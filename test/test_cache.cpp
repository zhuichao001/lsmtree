#include<iostream>
#include<string>
#include "sstable.h"

std::string basedir = "./";

void test(){
    const int COUNT = 10;
    sstable tab(1, 1001, nullptr, nullptr);
    tab.open();
    
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);
        
        std::string key=k;
        std::string val=v;
        int err = tab.put(i+1, key, val);
        if(err!=0){
            fprintf(stderr, "err:%d\n", err);
        }
    }

    tab.cache();

    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);
        
        std::string key=k;
        std::string val=v;
        std::string rval;
        tab.get(i+1, key, rval);

        if(val==rval){
            std::cout<<"yes same:"<<key<<std::endl;
        }else{
            std::cout<<"not same:"<<key<<":"<<rval <<", expect:"<<val<<std::endl;
        }
    }
}

int main(){
    test();
    return 0;
}
