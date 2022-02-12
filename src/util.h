#ifndef _LSMTREE_UTIL_H_
#define _LSMTREE_UTIL_H_

template <typename T>
bool compare_and_swap(T* object, T expected, T desired) {
    // TODO: use c11 atomic_compare_exchange_weak
    return __sync_bool_compare_and_swap(object, expected, desired);
}

#endif
