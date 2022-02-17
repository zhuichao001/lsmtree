#include<iostream>
#include<string>
#include "primarysst.h"

void test(){
    const int COUNT = 10;
    primarysst tab(1000);
    tab.create("./prisst.dat");
    
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);
        
        std::string key=k;
        std::string val=v;
        tab.put(i+1, key, val);

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
