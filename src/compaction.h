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
    int level_;

    compaction(int lev):
        level_(lev){
    }

    basetable *input(int w, int idx){
        return inputs_[w][idx];
    }

    void settle_inputs(version *ver);

    int level(){
        return level_;
    }

    int size(){
        return inputs_[0].size()+inputs_[1].size();
    }

    void print(){
        for(int i=0; i<2; ++i){
            for(basetable *t : inputs_[i]){
                fprintf(stderr, "  compaction input %i level-%d sst-%d <%s,%s>\n", i, t->level, t->file_number, t->smallest.c_str(), t->largest.c_str());
            }
        }
    }

};

#endif
