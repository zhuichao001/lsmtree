#ifndef __H_SKIP_LIST_H__
#define __H_SKIP_LIST_H__

#include <stddef.h>
#include <stdlib.h>
#include <limits.h>
#include <assert.h>
#include <iostream>
#include <string>

enum{
    FLAG_DEL = -1,
};

class skiplist;

class node {
    friend class skiplist;
    int height;
    node **forwards;  // next for i'th layer
public:
    std::string key;
    std::string val;
    int flag;

    node(const int level, const std::string &k, const std::string &v="", const int flagcode=0):
        height(level),
        key(k),
        val(v),
        flag(flagcode){
        assert(level>0);
        fprintf(stderr, "    new level:%d, key:%s, val:%s\n", level, k.c_str(), v.c_str());
        forwards = new node*[height];
        assert(forwards!=nullptr);
        for(int i=0; i<height; ++i){
            forwards[i] = nullptr;
        }
    }
    
    ~node(){
        delete []forwards;
    }

    node *next(){
        return forwards[0];
    }

    void print(){
        fprintf(stderr, "[node] key:%s, val:%s", key.c_str(), val.c_str());
        for(int i=0; i<height; ++i){
            fprintf(stderr, "    i:%d, next:%p\n", i, forwards[i]);
        }
    }
};

class skiplist {
    node *head;
    node *nil;

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
    class iterator{
    public:
        node *ptr;

        node * operator *(){
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

    skiplist(int maxh, int branch):
        MAXHEIGHT(maxh),
        BRANCHING(branch){
        length = 0;
        height = 1;
        head = new node(MAXHEIGHT, "");
        nil = new node(MAXHEIGHT, std::string(256, '\xff')); 
        for(int i=0; i<MAXHEIGHT; ++i){
            head->forwards[i] = nil;
        }
    }

    ~skiplist(){
        clear();
        delete nil;
        delete head;
    }

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

    int size(){ return length; }

    node *search(const std::string &k);

    node *insert(const std::string &k, const std::string &v, const int flag=0);

    void print();

    void clear();
};


#endif
