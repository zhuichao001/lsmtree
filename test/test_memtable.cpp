#include<iostream>
#include<string>
#include "memtable.h"

void test(){
    const int COUNT = 10000;
    memtable tab;
    
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);
        
        std::string key=k;
        std::string val=v;
        tab.put(i, 0, key, val);

        std::string rval;
        tab.get(0, key, rval);

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
