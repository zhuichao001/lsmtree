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
    version *ver_;
    int level_;
    std::string start_;
    std::string end_;
    basetable *from_;
    std::vector<basetable *> inputs_;
    void settle();
    public:
    compaction(version *ver, basetable *focus):
        ver_(ver),
        level_(focus->level+1),
        start_(focus->smallest),
        end_(focus->largest),
        from_(focus){
        settle();
    }

    int level(){
        return level_;
    }

    int size(){
        return inputs_.size();
    }

    std::vector<basetable *> &inputs(){
        return inputs_;
    }

    void print(){
        for(basetable *t : inputs_){
            fprintf(stderr, "    compaction input level-%d sst-%d <%s,%s>\n", t->level, t->file_number, t->smallest.c_str(), t->largest.c_str());
        }
    }
};

#endif
