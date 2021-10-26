#include "skiplist.h"


node *skiplist::search(const string &k){
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


node *skiplist::insert(const string &k, const string &v){
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
        return cur->forwards[0];
    } else { //insert
        ++length;

        const int H = this->level();
        for(int i=this->height; i<H; ++i) {
            updates[i] = this->head;
        }

        this->height = H>this->height ? H : this->height;
        
        node * neo = new node(H, k, v);
        for(int i=0; i<H; ++i){
            neo->forwards[i] = updates[i]->forwards[i];
            updates[i]->forwards[i] = neo;
        }
        return neo;
    }
}


void skiplist::print(){
    cout << "level:" << this->height << endl;
    for(int i=this->height-1; i>=0; --i){
        node *cur = this->head;
        while(cur != this->nil){
            cout << cur->key << ":" << cur->val << " |-> ";
            cur = cur->forwards[i];
        }
        cout << "nil" << endl;
    }
}
