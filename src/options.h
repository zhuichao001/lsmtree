#ifndef _LSMTREE_OPTIONS_H_
#define _LSMTREE_OPTIONS_H_

#include "snapshot.h"

//for open db
struct options{
    int memtable_size;
    int max_open_files;
};

///for read
struct roptions{
    bool fill_cache;
    const snapshot *snap;
    roptions(): snap(nullptr){
    }
};

struct woptions{
    bool sync;
};

#endif
