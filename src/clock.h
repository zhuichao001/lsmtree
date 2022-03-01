#include <time.h>


inline long get_time_sec(){
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec;
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
    return ts.tv_nsec;
}

inline long get_time_usec(){
    return get_time_sec()*1000+get_time_nsec()/1000;
}
