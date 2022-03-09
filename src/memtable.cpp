#include "memtable.h"

int memtable::get(const uint64_t seqno, const std::string &key, std::string &val){
    node<onval *> *cur = table_.search(key);
    while(cur!=nullptr){
        if(cur->key!=key){
            return -1;
        }
        if(cur->val->seqno > seqno){
            cur = cur->forwards[0];
        }else{
            val = cur->val->val;
            return 0;
        }
    }
    return -1;
}

int memtable::put(const int logidx, const uint64_t seqno, const std::string &key, const std::string &val, const uint8_t flag){
    size_ += key.size() + sizeof(onval);
    onval *vobj = new onval{logidx, seqno, val, flag};
    node<onval *> *neo = table_.insert(key, vobj);
    assert(neo!=nullptr);
    return 0;
}

int memtable::del(const int logidx, const uint64_t seqno, const std::string &key){
    const std::string val="";
    return put(logidx, seqno, key, val, FLAG_DEL);
}

int memtable::scan(const uint64_t seqno, std::function<int(int /*logidx*/, uint64_t /*seqno*/, const std::string &/*key*/, const std::string &/*val*/, int /*flag*/)> visit){
    for(skiplist<onval *>::iterator it = table_.begin(); it!=table_.end(); ++it){
        node<onval *> *p = *it;
        onval *v = p->val;
        visit(v->logidx, v->seqno, p->key, v->val, v->flag);
    }
    return 0;
}
