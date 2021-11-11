#ifndef __H_SKIP_LIST_H__
#define __H_SKIP_LIST_H__

#include <stddef.h>
#include <stdlib.h>
#include <limits.h>
#include <iostream>
#include <string>

enum{
    FLAG_DEL = -1,
};

class skiplist;

class node {
    friend class skiplist;
    int height;
    node **forwards;
public:
    std::string key;
    std::string val;
    int flag;

    node(const int level, const std::string &k, const std::string &v="", const int flagcode=0):
        key(k),
        val(v),
        flag(flagcode){
        forwards = new node*[level];
        for(int i=0; i<level; ++i){
            forwards[i] = nullptr;
        }
    }

    node *next(){
        return forwards[0];
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
    class iterator;
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
        nil = new node(MAXHEIGHT, std::string(2048, '\xff')); 
        for(int i=0; i<MAXHEIGHT; ++i){
            head->forwards[i] = nil;
        }
    }

    ~skiplist(){
        clear();
        delete head;
        delete nil;
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
