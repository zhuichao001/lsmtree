#ifndef _TAMPER_H_
#define _TAMPER_H_

#include <iostream>
#include <semaphore.h>
#include <unistd.h>
#include <memory>
#include <thread>


class tamper{
    lsmtree *lsm;
    sem_t immu_done;
    sem_t immu_come;
    bool running;
public:
    tamper(lsmtree *tree):
        lsm(tree),
        running(true){
        sem_init(/*sem_t *sem*/&busy, /*int pshared*/0, /*unsigned int value*/0);

        std::thread(&tamper::run, this);
    }

    void start(){
        sem_post(&immu_come);
    }

    void wait(){
        sem_wait(&immu_done);
    }

    void stop(){
        running = false;
    }

    void run(){
        while(running==true){
            sem_wait(&immu_come);
            lsm->compact_immutable;
            sem_post(&immu_done);
        }
    }
};

#endif
