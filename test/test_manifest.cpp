#include <iostream>
#include "fio.h"

void test0(){
    std::string data;
    if(read_file("./data/meta/MANIFEST-528341240", data)<0){
        perror("read meta:");
        return;
    }

    const char *SEPRATOR = "\0\0\0\0\0\0\n";
    char *token = strtok(const_cast<char*>(data.c_str()), SEPRATOR);
    while(token!=nullptr){
        int level, fnumber;
        char limit[2][64];
        memset(limit, 0, sizeof(limit));
        sscanf(token, "%d %d %s %s", &level, &fnumber, limit[0], limit[1]);
        
        fprintf(stderr, "%d %d %s %s\n", level, fnumber, limit[0], limit[1]);

        token = strtok(nullptr, SEPRATOR);
    }
}

int main(){
    test0();
    return 0;
}
