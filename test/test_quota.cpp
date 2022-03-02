#include <math.h>
#include <iostream>

const int SST_LIMIT = 2<<18; //default sst size:2MB

int max_level_size(int ln){
    if(ln==0){
        return 8 * SST_LIMIT;
    }
    return int(pow(10,ln)) * 8 * SST_LIMIT;
}

int main(){
    for(int i=0; i<9; ++i){
        int quota = max_level_size(i);
        std::cout<<"i="<<i<<", quota size=" << quota<<std::endl;
    }
    return 0;
}
