#ifndef _CAMPACTION_H_
#define _CAMPACTION_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <semaphore.h>
#include <pthread.h>
#include <functional> 
#include <memory>
#include <thread>
#include "basetable.h"


class version;

class compaction{
public:
    std::vector<basetable *> inputs[2];
    version *ver;
    int level;

    compaction(version *cur, int lev):
        ver(cur),
        level(lev){
    }

    basetable *input(int w, int idx){
        return inputs[w][idx];
    }

    //TODO: optimize
    void settle_inputs();
};

#endif
