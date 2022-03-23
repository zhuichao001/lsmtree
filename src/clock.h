#include <time.h>
#include <sys/time.h>
#include <string>


inline long get_time_sec(){
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec;
}

inline long get_time_msec(){
    struct timespec ts;
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    return ts.tv_sec*1000 + ts.tv_nsec/1000000;
}

inline long get_time_usec(){
    struct timespec ts;
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    return ts.tv_sec*1000000 + ts.tv_nsec/1000;
}

inline long get_time_nsec(){
    struct timespec ts;
    /*
    CLOCK_REALTIME
        System-wide realtime clock. Setting this clock requires appropriate privileges.
    CLOCK_MONOTONIC
        Clock that cannot be set and represents monotonic time since some unspecified starting point.
    CLOCK_PROCESS_CPUTIME_ID
        High-resolution per-process timer from the CPU.
    CLOCK_THREAD_CPUTIME_ID
        Thread-specific CPU-time clock.
    */
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    return ts.tv_sec*1000000000 + ts.tv_nsec;
}

inline std::string timestamp(){
    std::time_t t = std::time(NULL);
    return std::asctime(std::localtime(&t));
}
