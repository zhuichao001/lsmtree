#ifndef __H_SKIP_LIST_H__
#define __H_SKIP_LIST_H__

#include <stddef.h>
#include <stdlib.h>
#include <limits.h>
#include <assert.h>
#include <iostream>
#include <atomic>
#include <string>


template<class T>
class node {
    class skiplist;
    friend class skiplist;
public:
    std::string key;
    T val;
    int height;
    node<T> **forwards;  // next for i'th layer

    node(const int level, const std::string &k, const T &v):
        height(level),
        key(k),
        val(v) {
        assert(level>0);
        forwards = new node<T> *[height];
        assert(forwards!=nullptr);
        for(int i=0; i<height; ++i){
            forwards[i] = nullptr;
        }
    }
    
    ~node(){
        if(val){
            delete val;
        }
        delete []forwards;
    }

    node *next(){
        return forwards[0];
    }

    void print(){
        fprintf(stderr, "[node] key:%s\n", key.c_str());
        for(int i=0; i<height; ++i){
            fprintf(stderr, "    i:%d, next:%p\n", i, forwards[i]);
        }
    }
};

template<class T>
class skiplist {
    node<T> *head;
    node<T> *nil;

    int length;
    int height;

    const int MAXHEIGHT;
    const int BRANCHING;

    int level(){ // rand level
        int h = 1;
        while(h < MAXHEIGHT && rand()%BRANCHING == 0){
            ++h;
        }
        return h;
    }

public:

    skiplist(int maxh, int branch):
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

    ~skiplist(){
        clear();
        delete nil;
        delete head;
    }

    int size(){ return length; }

    node<T> *search(const std::string &k){
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

    node<T> *insert(const std::string &k, T &v){
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

    void print(){
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

    void clear(){
        node<T> *cur = head->forwards[0];
        while(cur!=nil){
            node<T> *tmp = cur;
            cur = cur->forwards[0];
            delete tmp;
        }
        head->forwards[0] = nil;
    }

public:
    class iterator{
        node<T> *ptr;
        friend class skiplist;
    public:
        node<T> * operator *(){
            return ptr;
        }

        iterator & operator ++(){
            ptr = ptr->next();
            return *this;
        }

        bool operator !=(const iterator &other){
            return ptr!=other.ptr;
        }
    };

    iterator begin(){
        iterator it;
        it.ptr = head->next();
        return it;
    }

    iterator end(){
        iterator it;
        it.ptr = nil;
        return it;
    }

};

#endif
