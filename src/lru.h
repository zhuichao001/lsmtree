#ifndef _LSMTREE_LRU_H_
#define _LSMTREE_LRU_H_

#include <map>
#include <string>
#include <iostream>

template<typename KT, typename VT>
class NodeList;

template<typename KT, typename VT>
class LRUCache;

template<typename KT, typename VT>
class Node{
    KT key;
    VT val;   

    Node<KT, VT> *prev, *next;
    bool fixed;

    friend NodeList<KT, VT>;
    friend LRUCache<KT, VT>;
public:
    Node():
        prev(nullptr),
        next(nullptr),
    fixed(true){
    };

    Node(const KT &k, const VT &v, bool pined):
        key(k),
        val(v),
    fixed(pined){
        prev = nullptr; 
        next = nullptr;
    }
};

template<typename KT, typename VT>
class NodeList{
    Node<KT, VT> head;
public:
    NodeList():
        head(){
        head.next = &head;
        head.prev = &head;
    }

    bool empty(){
        return head.next == &head;
    }

    void remove(Node<KT,VT> *node){
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    Node<KT,VT> *pop(){
        if(empty()){
            return nullptr;
        }
        Node<KT,VT> *res = head.prev;
        head.prev->next =  &head;
        head.prev = head.prev->prev;
        res->next = nullptr;
        res->prev = nullptr;
        return res;
    }

    void push(Node<KT,VT> *node){
        node->next = head.next;
        node->prev = &head;
        head.next->prev = node;
        head.next = node;
    }

    void update(Node<KT,VT> *node){
        node->next->prev = node->prev;
        node->prev->next = node->next;
        push(node);
    }

    void display(){
        Node<KT, VT> *node = head.next;
        while(node!=&head){
           std::cout<<"("<<node->key<<","<<node->val<<"), "; 
           node = node->next;
        }
        std::cout<<std::endl;
    }
};

template<typename KT, typename VT>
class LRUCache{
    NodeList<KT,VT> list;
    std::map<KT, Node<KT,VT>*> cache;
    int size;
    int capacity;
    std::mutex mutex;

public:
    LRUCache(int cap):
        size(0),
        capacity(cap),
        list(){
    }

    bool exist(const KT &key){
        return cache.find(key) != cache.end();
    }

    void setfixed(const KT &key, bool isfixed){
        auto it= cache.find(key) ;
        if(it==cache.end()){
            return;
        }
        it->second->fixed = isfixed;
    }

    int get(const KT &key, VT &val){
        std::unique_lock<std::mutex> lock{mutex};
        auto it = cache.find(key);
        if(it == cache.end()){
            return -1;
        }
        Node<KT,VT> *node = it->second;
        val = node->val;
        list.update(node);
        return 0;
    }

    void put(const KT &key, VT &val, bool fixed){
        std::unique_lock<std::mutex> lock{mutex};
        auto it = cache.find(key);
        if(it != cache.end()){ //update
            fprintf(stderr, "WARNING, LRU put %s, but has exist\n", key.c_str());
            Node<KT,VT> *node = it->second;
            node->val = val;
            list.update(node);
        }else{ //add
            if(size == capacity){
                Node<KT,VT> *node = nullptr;
                int n=capacity;
                while(--n>=0){ //TODO if all FIXED
                    node = list.pop();
                    if(node->fixed!=true){
                        break;
                    }
                    list.push(node);
                }
                if(n<0){
                    capacity+=1;
                } else {
                    assert(node!=nullptr);
                    fprintf(stderr, "LRU IS FULL, pop %s\n", node->key.c_str());
                    cache.erase(node->key);
                    VT v = node->val;
                    v->uncache();
                    delete node;
                    --size;
                }
            }

            Node<KT,VT> *node = new Node<KT,VT>(key, val, fixed);
            list.push(node);
            cache[key] = node;
            ++size;
        }
    }

    int del(const std::string &key, VT &val){
        std::unique_lock<std::mutex> lock{mutex};
        auto it = cache.find(key);
        if(it == cache.end()){
            return -1;
        }
        Node<KT,VT> *node = it->second;
        val = node->val;
        val->uncache();
        cache.erase(key);
        list.remove(node);
        delete node;
        --size;
        return 0;
    }

    void display(){
        list.display();
    }
};

#endif
