#ifndef _TAMPER_H_
#define _TAMPER_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <semaphore.h>
#include <pthread.h>
#include <functional>
#include <memory>
#include <thread>


class tamper{
    std::function<int(void)> callee;
    sem_t immu_done;
    sem_t immu_come;
    bool running;
    std::thread * daemon;

public:
    tamper(std::function<int(void)> f):
        callee(f),
        running(true){
        sem_init(/*sem_t *sem*/&immu_done, /*int pshared*/0, /*unsigned int value*/1);
        sem_init(/*sem_t *sem*/&immu_come, /*int pshared*/0, /*unsigned int value*/0);

        daemon  = new std::thread(&tamper::run, this);
    }

    ~tamper(){
        daemon->join();
        delete daemon;
    }

    void notify(){
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
            callee();
            sem_post(&immu_done);
        }
    }
};

#endif
