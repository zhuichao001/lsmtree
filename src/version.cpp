#include "version.h"

int max_level_size(int ln){
    if(ln==0){
        return 8 * SST_LIMIT;
    }
    return int(pow(10,ln)) * 8 * SST_LIMIT;
}

int version::get(const uint64_t seqno, const std::string &key, std::string &val){
    kvtuple res;
    res.seqno = 0;

    for(int j=0; j<ssts[0].size(); ++j){
        if(key<ssts[0][j]->smallest || key>ssts[0][j]->largest){
            continue;
        }

        kvtuple tmp;
        primarysst *t = dynamic_cast<primarysst*>(ssts[0][j]);
        t->ref();

        fprintf(stderr, "try find in level-0 sst-%d, %s\n", t->file_number, tmp.ckey);
        int err = t->get(seqno, key, tmp);
        t->unref();
        if(err<0){
            continue;
        }
        fprintf(stderr, "found in level-0 sst-%d, %s:%s\n", t->file_number, tmp.ckey, tmp.cval);
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

    for(int i=1; i<MAX_LEVELS; ++i){
        //std::vector<basetable*>::iterator it = std::lower_bound(ssts[i].begin(), ssts[i].end(), key, 
        //        [](const basetable *b, const std::string &k) { return b->smallest < k; });
        //if(it==ssts[i].end()){
        //    continue;
        //}
        for(int j=0; j<ssts[i].size(); ++j){
            sstable *t = dynamic_cast<sstable *>(ssts[i][j]);
            if(key<t->smallest || key>t->largest){
                continue;
            }
            t->ref();
            fprintf(stderr, "try find in level-%d sst-%d, %s\n", i, t->file_number, key.c_str());
            int err = t->get(seqno, key, val);
            t->unref();
            if(err==0 || err==ERROR_KEY_DELETED){
                t->allowed_seeks -= 1;
                if(t->allowed_seeks==0){
                    hot_sst = t;
                }
                fprintf(stderr, "found in level-%d sst-%d, %s:%s\n", i, t->file_number, key.c_str(), val.c_str());
                return 0;
            }
        }
    }
    return -1;
}

void version::calculate(){
    crownd_score = 0.0;
    crownd_level = -1;
    for(int i=0; i<MAX_LEVELS; ++i){
        double total = 0.0;
        for(int j=0; j<ssts[i].size(); ++j){
            total += ssts[i][j]->filesize();
        }
        const int quota_size = max_level_size(i);
        if(total/quota_size > crownd_score){
            crownd_score = total/quota_size;
            crownd_level = i;
        }
    }
    fprintf(stderr, "after calculate crownd score:%f, level:%d\n", crownd_score, crownd_level);
}

versionset::versionset():
    next_fnumber_(10000),
    last_sequence_(1),
    current_(nullptr){
    verhead_.prev = &verhead_;
    verhead_.next = &verhead_;
    this->appoint(new version(this));
}

compaction *versionset::plan_compact(){
    compaction *c;

    const bool size_too_big = current_->crownd_score >= 1.0;
    const bool seek_too_many = current_->hot_sst != nullptr;

    if (size_too_big) {
        int level = current_->crownd_level;
        fprintf(stderr, "plan to compact because size too big, level:%d\n", level);
        c = new compaction(level);
        for(int i=0; i < current_->ssts[level].size(); ++i){
            basetable *t = current_->ssts[level][i];
            if(campact_poles_[i].empty() || t->smallest >= campact_poles_[i]){ //TODO
                c->inputs_[0].push_back(t);
                break;
            }
        }

        if(c->inputs_[0].empty()){ //wrap arround to begin
            basetable *t = current_->ssts[level][0];
            c->inputs_[0].push_back(t);
        }
    } else if(seek_too_many) {
        fprintf(stderr, "plan to compact because seek too many, level:%d\n", current_->hot_sst->getlevel());
        c = new compaction(current_->hot_sst->getlevel());
        c->inputs_[0].push_back(current_->hot_sst);
    } else {
        return nullptr;
    }

    c->settle_inputs(current_);
    return c;
}

void versionset::apply(versionedit *edit){
    version *neo = new version(this);
    for(int i=0; i<MAX_LEVELS; ++i){
        std::vector<basetable*> &added = edit->addfiles[i];
        int k = 0;
        for(int j=0; j<current_->ssts[i].size(); ++j){
            basetable *t = current_->ssts[i][j];
            if(edit->delfiles.count(t)!=0){
                continue;
            }
            for(; k<added.size(); ++k){
                if(added[k]->smallest < t->smallest){
                    neo->ssts[i].push_back(added[k]);
                    added[k]->ref();
                    fprintf(stderr, "apply: add added sst-%d to level:%d [%s, %s]\n", added[k]->file_number, i, added[k]->smallest.c_str(), added[k]->largest.c_str());
                } else {
                    break;
                }
            }
            neo->ssts[i].push_back(t);
            t->ref();
        }
        for(; k<added.size(); ++k){
            neo->ssts[i].push_back(added[k]);
            added[k]->ref();
            fprintf(stderr, "apply: add added sst-%d to level:%d [%s, %s]\n", added[k]->file_number, i, added[k]->smallest.c_str(), added[k]->largest.c_str());
        }
    }

    this->appoint(neo);
}
