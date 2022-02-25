#include "lsmtree.h"
#include "sstable.h"
#include<algorithm>


thread_pool_t backstage(1);
std::string basedir;

const int TIER_SST_COUNT(int level){
    return TIER_PRI_COUNT * pow(10, level);
}

int lsmtree::open(const options *opt, const char *dirpath){
    basedir = dirpath;
    char pripath[64];
    sprintf(pripath, "%s/pri/\0", dirpath);
    if(!exist(pripath)){
        mkdir(pripath);
    }

    for(int lev=1; lev<=MAX_LEVELS; ++lev){
        char path[64];
        sprintf(path, "data/sst/%d/\0", lev);
        if(!exist(path)){
            mkdir(path);
        }
    }

    char metapath[64];
    sprintf(metapath, "%s/meta/\0", dirpath);
    if(!exist(metapath)){
        mkdir(metapath);
    }else{
        versions_.recover(metapath);
    }

    mutab_ = new memtable;
    mutab_->ref();
    return 0;
}

int lsmtree::get(const roptions &opt, const std::string &key, std::string &val){
    int seqno = (opt.snap==nullptr)? versions_.last_sequence():opt.snap->sequence();
    fprintf(stderr, "lsmtree::get seqno:%d\n", seqno);
    version *cur = nullptr; 
    {
        std::unique_lock<std::mutex> lock{mutex_};
        mutab_->ref();
        if(immutab_!=nullptr){
            immutab_->ref();
        }
        cur = versions_.current();
        cur->ref();
    }

    {
        val = "";
        int err = mutab_->get(seqno, key, val);
        mutab_->unref();
        if(err==0){
            fprintf(stderr, "ok, mutab get %s:%s\n", key.c_str(), val.c_str());
            return 0;
        }
    }

    if(immutab_!=nullptr){
        int err = immutab_->get(seqno, key, val);
        immutab_->unref();
        if(err==0){
            fprintf(stderr, "ok, immutab get %s:%s\n", key.c_str(), val.c_str());
            return 0;
        }
    }

    {
        int err = cur->get(seqno, key, val);
        cur->unref();
        schedule_compaction();
        fprintf(stderr, "err:%d, version get %s:%s\n", err, key.c_str(), val.c_str());
        return err;
    }
}

void lsmtree::schedule_compaction(){
    backstage.post([this]{
        {
            std::unique_lock<std::mutex> lock{mutex_};
            if(immutab_!=nullptr){
                this->minor_compact();
            }else if(versions_.need_compact()){
                this->major_compact();
            }
        }
        schedule_compaction();
    });
}

int lsmtree::sweep_space(){
    if(mutab_->size() < MAX_MEMTAB_SIZE){
        return 0;
    }

    std::unique_lock<std::mutex> lock{mutex_};
    if (immutab_!=nullptr) {
        fprintf(stderr, "level0 wait\n");
        level0_cv_.wait(lock);
    } else {
        //TODO log file
        immutab_ = mutab_;
        mutab_ = new memtable;
        mutab_->ref();
        schedule_compaction();
    }
    return 0;
}

int lsmtree::put(const woptions &opt, const std::string &key, const std::string &val){
    if(sweep_space()){
        fprintf(stderr, "shift space error\n");
        return -1;
    }
    int seqno = versions_.add_sequence(1);
    if(mutab_->put(seqno, key, val)<0){ //TODO: USE Writebatch
        return -1;
    }
    return 0;
}

int lsmtree::del(const woptions &opt, const std::string &key){
    if(sweep_space()){
        fprintf(stderr, "shift space error\n");
        return -1;
    }

    int seqno = versions_.add_sequence(1);
    if(mutab_->del(seqno, key)<0){
        return -1;
    }

    return 0;
}

int lsmtree::minor_compact(){
    if(immutab_==nullptr){
        return -1;
    }

    versionedit edit;
    primarysst *sst = new primarysst(versions_.next_fnumber());
    sst->open();
    immutab_->scan(versions_.last_sequence(), [=, &edit, &sst](const uint64_t seqno, const std::string &key, const std::string &val, int flag) ->int {
        if(sst->put(seqno, key, val, flag)==ERROR_SPACE_NOT_ENOUGH){
            fprintf(stderr, "minor compact into sst-%d range:[%s, %s]\n", sst->file_number, sst->smallest.c_str(), sst->largest.c_str());
            //sst->print(versions_.last_sequence());
            edit.add(0, sst);
            sst = new primarysst(versions_.next_fnumber());
            sst->open();
            sst->put(seqno, key, val, flag);
        }
        return 0;
    });
    fprintf(stderr, "minor compact into sst-%d range:[%s, %s]\n", sst->file_number, sst->smallest.c_str(), sst->largest.c_str());
    //sst->print(versions_.last_sequence());
    edit.add(0, sst);

    versions_.apply(&edit);
    versions_.current()->calculate();
    immutab_->unref();
    immutab_ = nullptr;

    level0_cv_.notify_all();

    return 0;
}

int lsmtree::major_compact(){
    compaction *c = versions_.plan_compact();
    if(c==nullptr){ //do nothing
        return 0;
    }

    versionedit edit;
    {
        std::vector<basetable::iterator> vec; //collect all iterators
        for(int i=0; i<2; ++i){
            for(int j=0; j<c->inputs_[i].size(); ++j){
                edit.remove(c->inputs_[i][j]);
                basetable::iterator it = c->inputs_[i][j]->begin();
                vec.push_back(it);
                fprintf(stderr, "  major compact %d %d  sst-%d\n", i, j, c->inputs_[i][j]->file_number);
            }
        }
        if(vec.empty()){
            return 0;
        }

        const int destlevel = c->level()+1; //compact into next level
        sstable *sst = new sstable(destlevel, versions_.next_fnumber());
        edit.add(destlevel, sst);

        const int total=vec.size(); 
        int produced = 0;
        std::string lastkey;
        make_heap(vec.begin(), vec.end(), basetable::compare_gt);
        while(!vec.empty()){
            basetable::iterator it = vec.front();
            pop_heap(vec.begin(), vec.end(), basetable::compare_gt);
            vec.pop_back(); //remove iterator

            kvtuple t;
            it.parse(t);
            it.next();
            if(it.valid()){
                vec.push_back(it);
                push_heap(vec.begin(), vec.end(), basetable::compare_gt);
            }

            if(produced>0 && lastkey==t.ckey && t.seqno<snapshots_.smallest_sn()){
                fprintf(stderr, "major compact drop kvtuple %s:%s %d\n", t.ckey, t.cval, t.flag);
                continue;
            }

            ++produced;
            lastkey = t.ckey;

            if(sst->put(t.seqno, std::string(t.ckey), std::string(t.cval), t.flag)==ERROR_SPACE_NOT_ENOUGH){
                fprintf(stderr, "major compact into sst-%d range:[%s, %s]\n", sst->file_number, sst->smallest.c_str(), sst->largest.c_str());
                //sst->print(versions_.last_sequence());

                sst = new sstable(destlevel, versions_.next_fnumber());
                edit.add(destlevel, sst);
                sst->put(t.seqno, std::string(t.ckey), std::string(t.cval), t.flag);
            }
        }
        fprintf(stderr, "major compact into sst-%d range:[%s, %s], produced:%d->%d\n", sst->file_number, sst->smallest.c_str(), sst->largest.c_str(), total, produced);
        //sst->print(versions_.last_sequence());//TODO
    }

    versions_.apply(&edit);
    versions_.current()->calculate();

    return 0;
}

int lsmtree::write(const wbatch &bat){
    bat.scan([this](const char *key, const char *val, const int flag)->int{
        if(sweep_space()){
            fprintf(stderr, "failed shift space for write batch.\n");
            return -1;
        }
        int seqno = versions_.add_sequence(1);
        if(flag==FLAG_VAL){
            return mutab_->put(seqno, key, val);
        }else{
            return mutab_->del(seqno, key);
        }
    });
    return 0;
}

snapshot * lsmtree::create_snapshot(){
    return snapshots_.create(versions_.last_sequence());
}

int lsmtree::release_snapshot(snapshot *snap){
    return snapshots_.destroy(snap);
}

