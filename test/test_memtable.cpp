#include<iostream>
#include<string>
#include "memtable.h"

const int COUNT = 500000;
memtable tab;

void test(){
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);
        std::string key=k;
        std::string val=v;

        tab.put(i, 0, key, val);
    }

    for(memtable::iterator it = tab.begin(); it.valid(); it.next()){
        onval *v = it.val();
        fprintf(stderr, "logidx:%d, seqno:%d, %s:%s, flag:%d\n", v->logidx, v->seqno, v->key.c_str(), v->val.c_str(), v->flag);
    }
}

void test_r(){
    for(int i=0; i<COUNT; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);
        std::string key=k;
        std::string val=v;

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
    tab.print(9999);
    std::cout << "=========" <<std::endl;
    test();
    return 0;
}
