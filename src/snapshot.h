#ifndef _LSMTREE_SNAPSHOT_H_
#define _LSMTREE_SNAPSHOT_H_

#include <stdint.h>
#include <mutex>

typedef uint64_t serialnum;

class snapshot{
public:
    snapshot(serialnum sn):
        sn_(sn){
    }

    serialnum sequence() const { return sn_;}
private:
    friend class snapshotlist;
    snapshot *prev_;
    snapshot *next_;

    const serialnum sn_;
};

class snapshotlist{
private:
    snapshot head_;
    std::mutex mutex_;

public:
    snapshotlist():
        head_(0){
        head_.prev_ = &head_;
        head_.next_ = &head_;
    }

    snapshot *create(serialnum sn){
        std::unique_lock<std::mutex> lock{mutex_};
        snapshot *shot = new snapshot(sn);

        //append to tail
        shot->next_ = &head_;
        shot->prev_ = head_.prev_;
        head_.prev_->next_ = shot;
        head_.prev_ = shot;
        return shot;
    }

    int destroy(const snapshot *shot){
        std::unique_lock<std::mutex> lock{mutex_};
        shot->prev_->next_ = shot->next_;
        shot->next_->prev_ = shot->prev_;
        delete shot;
        return 0;
    }

    int smallest_sn(){
        std::unique_lock<std::mutex> lock{mutex_};
        if(head_.prev_==&head_){
            return -1;
        }
        return head_.prev_->sequence();
    }

    bool empty(){ return head_.next_==&head_; }

    snapshot *newest() const {
        return head_.prev_;
    }

    snapshot *oldest() const {
        return head_.next_;
    }
};

#endif
