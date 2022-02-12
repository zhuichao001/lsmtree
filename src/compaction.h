#ifndef _CAMPACTION_H_
#define _CAMPACTION_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <semaphore.h>
#include <pthread.h>
#include <functional> 
#include <vector>
#include <memory>
#include <thread>
#include "basetable.h"


class version;

class compaction{
public:
    std::vector<basetable *> inputs_[2];
    version *ver;
    int level_;

    compaction(version *cur, int lev):
        ver(cur),
        level_(lev){
    }

    basetable *input(int w, int idx){
        return inputs_[w][idx];
    }

    //TODO: optimize
    void settle_inputs();

    int level(){
        return level_;
    }

};

#endif