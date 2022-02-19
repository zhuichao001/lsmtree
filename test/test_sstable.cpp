#include<iostream>
#include<string>
#include "sstable.h"


void test(){
    const int COUNT = 3;
    sstable tab(1, 1001);
    tab.create("./sst.dat");
    
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


    for(int i=COUNT-1; i>=0; --i){
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
