#include "version.h"

int max_level_size(int ln){
    if(ln==0){
        return 8 * SST_LIMIT;
    }
    return int(pow(10,ln)) * SST_LIMIT;
}

int version::get(const uint64_t seqno, const std::string &key, std::string &val){
    kvtuple res;
    for(int j=0; j<ssts[0].size(); ++j){
        if(key<ssts [0][j]->smallest || key>ssts[0][j]->largest){
            continue;
        }

        kvtuple tmp;
        primarysst *t = dynamic_cast<primarysst*>(ssts[0][j]);
        if(t->get(seqno, key, tmp)<0){
            continue;
        }
        if(tmp.seqno>res.seqno){
            res = tmp;
        }
    }
    if(res.seqno>0){
        if(res.flag==FLAG_VAL){
            val = res.cval;
            return 0;
        }else{
            return -1;
        }
    }

    for(int i=0; i<MAX_LEVELS; ++i){
        std::vector<basetable*>::iterator it = std::lower_bound(ssts[i].begin(), ssts[i].end(), key, 
                [](const basetable *b, const std::string &k) { return b->smallest < k; });
        if(it==ssts[i].end()){
            continue;
        }

        sstable *t = dynamic_cast<sstable *>(*it);
        t->ref();
        int err = t->get(seqno, key, val);
        t->unref();
        if(err==0 || err==ERROR_KEY_DELETED){
            t->allowed_seeks -= 1;
            if(t->allowed_seeks==0){
                hot_sst = t;
            }
            return 0;
        }
    }
    return -1;
}

void version::calculate(){
    crownd_score = 0.0;
    crownd_level = -1;
    for(int i=0; i<MAX_LEVELS; ++i){
        int total = 0;
        for(int j=0; j<ssts[i].size(); ++j){
            total += ssts[i][j]->filesize();
        }
        const int quota_size = max_level_size(i);
        if(total/quota_size > crownd_score){
            crownd_score = total/quota_size;
            crownd_level = i;
        }
    }
}

compaction *versionset::plan_compact(){
    compaction *c;
    int level = -1;

    const bool size_too_big = current_->crownd_score >= 1.0;
    const bool seek_too_many = current_->hot_sst != nullptr;

    if (size_too_big) {
        level = current_->crownd_level;
        c = new compaction(current_, level);
        for(int i=0; i < current_->ssts[level].size(); ++i){
            basetable *t = current_->ssts[level][i];
            if(campact_poles_[i].empty() || t->smallest >= campact_poles_[i]){
                c->inputs[0].push_back(t);
                break;
            }
        }

        if(c->inputs[0].empty()){ //wrap arround to begin
            basetable *t = current_->ssts[level][0];
            c->inputs[0].push_back(t);
        }
    } else if(seek_too_many) {
        c = new compaction(current_, current_->hot_sst->getlevel());
        c->inputs[0].push_back(current_->hot_sst);
    } else {
        return nullptr;
    }

    c->ver = current_;
    c->ver->ref();

    c->settle_inputs();
    return c;
}

void versionset::apply(versionedit *edit){
    version *neo = new version;
    version * cur = current();

    for(int i=0; i<MAX_LEVELS; ++i){
        std::vector<basetable*> &added = edit->addfiles[i];
        int k = 0;
        for(int j=0; j<cur->ssts[i].size(); ++j){
            basetable *t = cur->ssts[i][j];
            if(edit->delfiles.count(t)!=0){
                continue;
            }
            for(; k<added.size(); ++k){
                if(added[k]->smallest < t->smallest){
                    neo->ssts[i].push_back(added[k]);
                }
            }
            neo->ssts[i].push_back(cur->ssts[i][j]);
        }
    }
    current_ = neo;
}
