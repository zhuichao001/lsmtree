#include <algorithm>
#include "skiplist.h"

template<class T>
skiplist<T>::skiplist(int maxh, int branch):
    MAXHEIGHT(maxh),
    BRANCHING(branch){
    length = 0;
    height = 1;
    head = new node<T>(MAXHEIGHT, "", nullptr);
    nil = new node<T>(MAXHEIGHT, std::string(256, '\xff'), nullptr); 
    for(int i=0; i<MAXHEIGHT; ++i){
        head->forwards[i] = nil;
    }
}

template<class T>
skiplist<T>::~skiplist(){
    clear();
    delete nil;
    delete head;
}

template<class T>
node<T> *skiplist<T>::search(const std::string &k){
    node<T> *cur = this->head;
    for(int i=this->height-1; i>=0; --i){
        while(cur->forwards[i]->key<k){
            cur = cur->forwards[i];
        }
        if(cur->forwards[i]->key==k){
            return cur->forwards[i];
        }
    }
    return nullptr;
}

template<class T>
node<T> *skiplist<T>::insert(const std::string &k, T v){
    node<T> *updates[this->MAXHEIGHT];
    node<T> *cur = this->head;
    for(int i=this->height-1; i>=0; --i){
        while(cur->forwards[i]->key < k){
            cur = cur->forwards[i];
        }
        updates[i] = cur;
    }

    //insert
    const int H = this->level();
    node<T> *neo = new node<T>(H, k, v);
    if(neo==nullptr){
        fprintf(stderr, "can't malloc when insert");
        return nullptr;
    }
    for(int i=0; i<std::min(this->height, H); ++i){
        neo->forwards[i] = updates[i]->forwards[i];
        updates[i]->forwards[i] = neo;
    }

    for(int i=this->height; i<H; ++i) {
        neo->forwards[i] = nil;
        head->forwards[i] = neo;
    }
    this->height = std::max(this->height, H);
    ++length;
    return neo;
}

template<class T>
void skiplist<T>::clear(){
    node<T> *cur = head->forwards[0];
    while(cur!=nil){
        node<T> *tmp = cur;
        cur = cur->forwards[0];
        delete tmp;
    }
    head->forwards[0] = nil;
}

template<class T>
void skiplist<T>::print(){
    std::cout << "level:" << this->height << std::endl;
    for(int i=this->height-1; i>=0; --i){
        node<T> *cur = this->head;
        while(cur != this->nil){
            std::cout << cur->key << ":" << cur->val << " |-> ";
            cur = cur->forwards[i];
        }
        std::cout << "nil" << std::endl;
    }
}
