#include "version.h"
#include "clock.h"


inline uint64_t max_level_size(int ln){
    return (ln==0)? 8*SST_LIMIT : uint64_t(pow(10,ln))*SST_LIMIT;
}

version::version(versionset *vs):
    vset(vs),
    refnum(0),
    tricky_sst(nullptr){
}

version::~version(){
    for(int i=0; i<MAX_LEVELS; ++i){
        for(basetable *t : ssts[i]){
            if(t->refnum()==1){
                fprintf(stderr, "REMOVE CACHE AND FILE, sst-%d\n", t->file_number);
                vset->cacheout(t);
                t->remove(); //TODO: async
            }
            t->unref();
        }
    }
}

int version::get(const uint64_t seqno, const std::string &key, std::string &val){
    kvtuple res;
    res.seqno = 0;
    for(int j=0; j<ssts[0].size(); ++j){
        primarysst *t = dynamic_cast<primarysst*>(ssts[0][j]);
        if(key<t->smallest || key>t->largest){
            continue;
        }

        vset->cachein(t);

        kvtuple tmp;
        int err = t->get(seqno, key, tmp);
        if(err<0){
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

    for(int i=1; i<MAX_LEVELS; ++i){
        std::vector<basetable*>::iterator it = std::lower_bound(ssts[i].begin(), ssts[i].end(), key, 
                [](const basetable *b, const std::string &k) { return b->largest < k; });
        if(it==ssts[i].end()){
            continue;
        }

        basetable *t = *it;
        if(key<t->smallest || key>t->largest){
            continue;
        }

        vset->cachein(t);

        int err = t->get(seqno, key, val);
        if(err==0 || err==ERROR_KEY_DELETED){
            if(t->allowed_seeks==0){
                tricky_sst = t;
            }
            return 0;
        }else{ //miss seek
            t->allowed_seeks -= 1;
        }
    }
    return -1;
}

void version::select_sst(const int level, const std::string &start, const std::string &end, std::vector<basetable*> &out){
    for (basetable *t : ssts[level]) {
        if (!t->overlap(start, end)) {
            continue;
        }
        out.push_back(t);
    }
    return;
}

void version::calculate(){
    crownd_score = 0.0;
    crownd_level = -1;
    for(int i=0; i<MAX_LEVELS; ++i){
        int total = 0;
        for(int j=0; j<ssts[i].size(); ++j){
            total += ssts[i][j]->filesize();
        }
        const double rate = (double)total/max_level_size(i);
        if(rate > crownd_score){
            crownd_score = rate;
            crownd_level = i;
        }
    }
    fprintf(stderr, "after calculate crownd score:%f, level:%d\n", crownd_score, crownd_level);
}

versionset::versionset():
    next_fnumber_(10000),
    last_sequence_(1),
    apply_logidx_(0),
    verhead_(this) {
    verhead_.prev = &verhead_;
    verhead_.next = &verhead_;
    this->appoint(new version(this));
}

void versionset::appoint(version *ver){
    version *old = current_;
    ver->ref();
    //TODO: memory barrier
    current_ = ver;
    if(old!=nullptr){
        old->unref();
    }

    //append current_ to tail
    ver->next = &verhead_;
    ver->prev = verhead_.prev;
    verhead_.prev->next = ver;
    verhead_.prev = ver;
}

compaction *versionset::plan_compact(version *ver){
    const bool size_too_big = ver->crownd_score >= 1.0;
    const bool miss_too_many = ver->tricky_sst != nullptr;
    basetable *focus = nullptr;
    int level = 0;
    if(size_too_big){
        level = ver->crownd_level;
        fprintf(stderr, "plan to compact because size too big, level:%d\n", level);
        for(basetable *t : ver->ssts[level]){
            if(roller_key_[level].empty() || t->smallest > roller_key_[level]){
                focus = t;
                break;
            }
        }

        if(focus==nullptr){ //roll back
            focus = ver->ssts[level][0];
        }
        compaction *c = new compaction(ver, focus); //compact into next level
        roller_key_[level] = focus->largest;
        return c;
    }else if(miss_too_many){
        fprintf(stderr, "plan to compact because seek too many, level:%d\n", current_->tricky_sst->level);
        focus = ver->tricky_sst;
        ver->tricky_sst->allowed_seeks = MAX_ALLOWED_SEEKS;
        ver->tricky_sst = nullptr;

        compaction *c = new compaction(ver, focus); //compact into next level
        if(c->size()==1){
            delete c;
            return nullptr;
        }
        return c;
    }else{
        return nullptr;
    }
}

version *versionset::apply(versionedit *edit){
    version *cur = current();
    version *neo = new version(this);
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

    return neo;
}

int versionset::persist(version *ver){ 
    char manifest_path[PATH_LEN];
    sprintf(manifest_path, "%s/MANIFEST-%ld\0", metapath_, get_time_usec());
    {
        int fd = open_create(manifest_path);
        fprintf(stderr, "persist MANIFEST: %s\n", manifest_path);
        for(int level=0; level<MAX_LEVELS; ++level) {
            for(int j=0; j<ver->ssts[level].size(); ++j){
                basetable *t = ver->ssts[level][j];
                char line[256];
                sprintf(line, "%d %d %s %s %d\n", level, t->file_number, t->smallest.c_str(), t->largest.c_str(), t->key_num);
                write_file(fd, line, strlen(line));
            }
        }
        ::close(fd);
    }

    char temporary[PATH_LEN];
    sprintf(temporary, "%s/.temporary\0", basedir.c_str());

    char data[256];
    sprintf(data, "%d %d %s\0", apply_logidx_, last_sequence_, manifest_path);
    write_file(temporary, data, strlen(data)); 
    char current[PATH_LEN];
    sprintf(current, "%s/CURRENT\0", basedir.c_str());
    fprintf(stderr, "MOVE MANIFEST from %s to %s\n", temporary, current);
    if(rename_file(temporary, current)<0){
        fprintf(stderr, "rename file failed\n");
    }
    return 0;
}

int versionset::recover(){
    sprintf(metapath_, "%s/meta/\0", basedir.c_str());
    if(!exist(metapath_)){
        mkdir(metapath_);
        return -1;
    }

    char curpath[PATH_LEN];
    sprintf(curpath, "%s/CURRENT\0", basedir.c_str());

    char manifest_path[PATH_LEN];
    memset(manifest_path, 0, sizeof(manifest_path));
    std::string data;
    if(read_file(curpath, data)<0){
        return -1;
    }
    sscanf(data.c_str(), "%d %d %s\0", &apply_logidx_, &last_sequence_, manifest_path);
    fprintf(stderr, "last_seqno:%d, manifest_path:%s\n", last_sequence_, manifest_path);

    versionedit edit;
    data.clear();
    if(read_file(manifest_path, data)<0){
        fprintf(stderr, "read manifest error\n");
        return -1;
    }

    const char *SEPRATOR = "\n";
    char *token = strtok(const_cast<char*>(data.c_str()), SEPRATOR);
    while(token!=nullptr){
        int level=0, fnumber=0, keynum=0;
        char limit[2][64];
        memset(limit, 0, sizeof(limit));
        sscanf(token, "%d %d %s %s %s %d", &level, &fnumber, limit[0], limit[1], &keynum);
        fprintf(stderr, "RECOVER sstable level-%d sst-%d <%s,%s>\n", level, fnumber, limit[0], limit[1]);

        basetable *sst;
        if(level>0){
            sst = new sstable(level, fnumber, limit[0], limit[1], keynum);
        }else{
            sst = new primarysst(fnumber, limit[0], limit[1], keynum);
        }
        sst->open();
        edit.add(level, sst);

        token = strtok(nullptr, SEPRATOR);
    }
    apply(&edit);
    return 0;
}
