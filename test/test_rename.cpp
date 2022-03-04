#include "fio.h"

void test(){
    const char *basedir = "data";
    const char * metapath = "./data/meta/";
    char temporary[128];
    sprintf(temporary, "%s/temporary\0", basedir);
    write_file(temporary, metapath, strlen(metapath)); 
    //char current[128];
    //sprintf(current, "%s/CURRENT\0", basedir);
    //fprintf(stderr, "MV MANIFEST from %s to %s\n", temporary, current);
    //rename_file(temporary, current);
}

int main(){
    test();
    return 0;
}
