#include "skiplist.h"
#include <algorithm>


node *skiplist::search(const std::string &k){
    node *cur = this->head;
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


node *skiplist::insert(const std::string &k, const std::string &v, const int flag){
    node *updates[this->MAXHEIGHT];
    node *cur = this->head;

    for(int i=this->height-1; i>=0; --i){
        while(cur->forwards[i]->key < k){
            cur = cur->forwards[i];
        }
        updates[i] = cur;
    }

    if(cur->forwards[0]->key==k) { //update
        cur->forwards[0]->val = v;
        cur->forwards[0]->flag = flag;
        return cur->forwards[0];
    } else { //insert
        const int H = this->level();
        node * neo = new node(H, k, v, flag);
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
}


void skiplist::clear(){
    node *cur = head;
    while(cur->forwards[0]!=nil){
        node *tmp = cur;
        cur = cur->forwards[0];
        delete tmp;
    }
    head->forwards[0] = nil;
}


void skiplist::print(){
    std::cout << "level:" << this->height << std::endl;
    for(int i=this->height-1; i>=0; --i){
        node *cur = this->head;
        while(cur != this->nil){
            std::cout << cur->key << ":" << cur->val << " |-> ";
            cur = cur->forwards[i];
        }
        std::cout << "nil" << std::endl;
    }
}
