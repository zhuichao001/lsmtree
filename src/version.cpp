#include "version.h"
#include "clock.h"


const int PATH_LEN = 64;

inline uint64_t max_level_size(int ln){
    return (ln==0)? 8*SST_LIMIT : uint64_t(pow(10,ln))*SST_LIMIT;
}

version::version(versionset *vs):
    vset(vs),
    refnum(0),
    hot_sst(nullptr){
}

version::~version(){
    for(int i=0; i<MAX_LEVELS; ++i){
        for(basetable *t : ssts[i]){
            if(t->refnum()==1){
                vset->cache_.evict(std::string(t->path));
                t->remove();
            }
            t->unref();
        }
    }
}

int version::get(const uint64_t seqno, const std::string &key, std::string &val){
    kvtuple res;
    res.seqno = 0;
    for(int j=0; j<ssts[0].size(); ++j){
        if(key<ssts[0][j]->smallest || key>ssts[0][j]->largest){
            continue;
        }

        primarysst *t = dynamic_cast<primarysst*>(ssts[0][j]);
        t->ref();
        if(!t->iscached()){
            vset->cache_.insert(std::string(t->path), t);
        }

        kvtuple tmp;
        int err = t->get(seqno, key, tmp);
        t->unref();
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
        t->ref();
        if(!t->iscached()){
            vset->cache_.insert(std::string(t->path), t);
        }
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
        const uint64_t quota_size = max_level_size(i);
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
    verhead_(this) {
    verhead_.prev = &verhead_;
    verhead_.next = &verhead_;
    this->appoint(new version(this));
}

compaction *versionset::plan_compact(){
    compaction *c;
    const bool size_too_big = current_->crownd_score >= 1.0;
    const bool seek_too_many = current_->hot_sst != nullptr;
    if(size_too_big){
        int level = current_->crownd_level;
        fprintf(stderr, "plan to compact because size too big, level:%d\n", level);
        c = new compaction(level+1); //compact into next level
        for(int i=0; i < current_->ssts[level].size(); ++i){
            basetable *t = current_->ssts[level][i];
            if(roller_key_[i].empty() || t->smallest > roller_key_[i]){
                c->inputs_[0].push_back(t);
                break;
            }
        }

        if(c->inputs_[0].empty()){ //roll back
            basetable *t = current_->ssts[level][0];
            c->inputs_[0].push_back(t);
        }
    }else if(seek_too_many){
        fprintf(stderr, "plan to compact because seek too many, level:%d\n", current_->hot_sst->level);
        c = new compaction(current_->hot_sst->level);
        c->inputs_[0].push_back(current_->hot_sst);
        current_->hot_sst->allowed_seeks = MAX_ALLOWED_SEEKS;
        current_->hot_sst = nullptr;
    }else{
        return nullptr;
    }

    c->settle_inputs(current_);
    if(seek_too_many){
        if(c->inputs_[1].size()==0){
            delete c;
            return nullptr;
        }
    }else{
        for(int i=0; i<2; ++i){ //roll compact_key ahead
            if(c->inputs_[i].size()>0){
                basetable *t = c->inputs_[i].back();
                roller_key_[t->level] = t->largest;
            }
        }
    }
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

    this->persist(neo->ssts);

    this->appoint(neo);
}

int versionset::persist(const std::vector<basetable*> ssts[MAX_LEVELS]){ 
    char metapath[PATH_LEN];
    sprintf(metapath, "%s/meta/\0", basedir.c_str());
    if(!exist(metapath)){
        mkdir(metapath);
    }

    char manifest_path[PATH_LEN];
    sprintf(manifest_path, "%s/meta/MANIFEST-%ld\0", basedir.c_str(), get_time_usec());
    int fd = open_create(manifest_path);

    fprintf(stderr, "persist MANIFEST: %s\n", manifest_path);

    for(int level=0; level<MAX_LEVELS; ++level) {
        for(int j=0; j<ssts[level].size(); ++j){
            basetable *t = ssts[level][j];
            char line[256];
            sprintf(line, "%d %d %s %s\n", level, t->file_number, t->smallest.c_str(), t->largest.c_str());
            write_file(fd, line, strlen(line));
        }
    }
    ::close(fd);

    char temporary[PATH_LEN];
    sprintf(temporary, "%s/.temporary\0", basedir.c_str());

    char data[256];
    sprintf(data, "%d %s\0", last_sequence_, manifest_path);
    write_file(temporary, data, strlen(data)); 
    char current[PATH_LEN];
    sprintf(current, "%s/CURRENT\0", basedir.c_str());
    fprintf(stderr, "MV MANIFEST from %s to %s\n", temporary, current);
    return rename_file(temporary, current);
}

int versionset::recover(){
    char metapath[PATH_LEN];
    sprintf(metapath, "%s/meta/\0", basedir.c_str());
    if(!exist(metapath)){
        mkdir(metapath);
        return 0;
    }

    char current[PATH_LEN];
    sprintf(current, "%s/CURRENT\0", basedir.c_str());
    char manifest_path[PATH_LEN];
    memset(manifest_path, 0, sizeof(manifest_path));

    std::string data;
    if(read_file(current, data)<0){
        return -1;
    }
    sscanf(data.c_str(), "%d %s\0", &last_sequence_, manifest_path);
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
        int level=0, fnumber=0;
        char limit[2][64];
        memset(limit, 0, sizeof(limit));
        fprintf(stderr, "token:[%s]\n", token);
        sscanf(token, "%d %d %s %s", &level, &fnumber, limit[0], limit[1]);
        fprintf(stderr, "RECOVER sstable level-%d sst-%d <%s,%s>\n", level, fnumber, limit[0], limit[1]);

        basetable *sst;
        if(level>0){
            sst = new sstable(level, fnumber, limit[0], limit[1]);
        }else{
            sst = new primarysst(fnumber, limit[0], limit[1]);
        }
        sst->open();
        edit.add(level, sst);

        token = strtok(nullptr, SEPRATOR);
    }
    apply(&edit);
    return 0;
}
