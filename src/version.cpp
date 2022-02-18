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
            //fprintf(stderr, "ignore because %s not in %d/%d sst-%d, [%s, %s]\n", key.c_str(), j, ssts[0].size(), ssts[0][j]->file_number, ssts[0][j]->smallest.c_str(), ssts[0][j]->largest.c_str());
            continue;
        }

        kvtuple tmp;
        primarysst *t = dynamic_cast<primarysst*>(ssts[0][j]);
        t->ref();
        //fprintf(stderr, "try find %s in %d/%d sst-%d\n", key.c_str(), j, ssts[0].size(), t->file_number);
        //t->print(seqno);

        int err = t->get(seqno, key, tmp);
        t->unref();
        if(err<0){
            continue;
        }
        fprintf(stderr, "found in %d sst-%d, %s:%s\n", j, t->file_number, tmp.ckey, tmp.cval);
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
    this->append(new version(this));
}

compaction *versionset::plan_compact(){
    compaction *c;

    const bool size_too_big = current_->crownd_score >= 1.0;
    const bool seek_too_many = current_->hot_sst != nullptr;

    if (size_too_big) {
        int level = current_->crownd_level;
        fprintf(stderr, "size too big, level:%d\n", level);
        c = new compaction(current_, level);
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
        fprintf(stderr, "seek too many, level:%d\n", current_->hot_sst->getlevel());
        c = new compaction(current_, current_->hot_sst->getlevel());
        c->inputs_[0].push_back(current_->hot_sst);
    } else {
        return nullptr;
    }

    //c->ver = current_;
    //c->ver->ref();

    c->settle_inputs();
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
                //fprintf(stderr, "apply: ignore deleted sst-%d, [%s, %s]\n", t->file_number, t->smallest.c_str(), t->largest.c_str());
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
            //fprintf(stderr, "apply: add original sst-%d, [%s, %s]\n", t->file_number, t->smallest.c_str(), t->largest.c_str());
            neo->ssts[i].push_back(t);
            t->ref();
        }
        for(; k<added.size(); ++k){
            neo->ssts[i].push_back(added[k]);
            added[k]->ref();
            //fprintf(stderr, "apply: add added sst-%d \n", added[k]->file_number);
        }
    }

    fprintf(stderr, "|||current size:%d, added size:%d, new size:%d\n", 
            current_->ssts[0].size(), edit->addfiles[0].size(), neo->ssts[0].size());

    this->append(neo);
}
