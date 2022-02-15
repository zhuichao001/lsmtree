#include "lsmtree.h"
#include "sstable.h"
#include<algorithm>


thread_pool_t backstage(1);

const int TIER_SST_COUNT(int level){
    return TIER_PRI_COUNT * pow(10, level);
}

int lsmtree::open(const options *opt, const char *basedir){
    sprintf(pripath, "%s/pri/\0", basedir);
    sprintf(sstpath, "%s/sst/\0", basedir);
    if(!exist(basedir)){
        mkdir(pripath);
        mkdir(sstpath);
        return 0;
    }

    /*

    std::vector<std::string> files; //temporary
    ls(pripath, files);
    for(auto path: files){
        primarysst *pri = new primarysst;
        pri->load(path.c_str());
        levels[0].push_back(pri);
    }

    files.clear();

    ls(sstpath, files);
    for(auto path: files){
        int level = atoi(path.c_str()+strlen(sstpath));
        sstable *sst = new sstable(level);
        sst->load(path.c_str());
        levels[level-1].push_back(sst);
    }

    //sort sst in every level 
    for(int i=0; i<MAX_LEVELS; ++i){
        sort_sst(i);
    }
    */
    return 0;
}

int lsmtree::get(const roptions &opt, const std::string &key, std::string &val){
    int seqno = (opt.snap==nullptr)? versions_->last_sequence():opt.snap->sequence();
    {
        mutab_->ref();
        int err = mutab_->get(seqno, key, val);
        mutab_->unref();
        if(err==0){
            return 0;
        }
    }

    if(immutab_!=nullptr){
        immutab_->ref();
        int err = immutab_->get(seqno, key, val);
        immutab_->unref();
        if(err==0){
            return 0;
        }
    }

    {
        version *cur = versions_->current();
        cur->ref();
        int err = cur->get(seqno, key, val);
        schedule_compaction();
        cur->unref();
        return err;
    }
}

void lsmtree::schedule_compaction(){
    if(!compare_and_swap(&compacting_, false, true)){
        return;
    }

    if(immutab_!=nullptr){
        backstage.post([this]{this->minor_compact();});
        return;
    }

    //TODO: deal manual compaction
    if(versions_->need_compact()){
        backstage.post([this]{this->major_compact();});
        return;
    }
}

int lsmtree::sweep_space(){
    if(mutab_->size() < MAX_MEMTAB_SIZE){
        return 0;
    }

    std::unique_lock<std::mutex> lock{mutex_};
    if (immutab_!=nullptr) {
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

    int seqno = versions_->add_sequence(1);
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

    int seqno = versions_->add_sequence(1);
    if(mutab_->del(seqno, key)<0){
        return -1;
    }

    return 0;
}

int lsmtree::minor_compact(){
    assert(immutab_!=nullptr);

    primarysst *sst = create_primarysst(versions_->next_fnumber());
    immutab_->scan(versions_->last_sequence(), [=](const uint64_t seqno, const std::string &key, const std::string &val, int flag) ->int {
        sst->put(seqno, key, val, flag);
        return 0;
    });

    versionedit edit;
    edit.add(0, sst);
    sst->ref();

    version *cur = versions_->current();
    cur->ref();
    versions_->apply(&edit);
    cur->unref();
    immutab_->unref();

    assert(compacting_==true);
    compacting_ = false;

    level0_cv_.notify_all();
    fprintf(stderr, "complete minor compact!!!");
    return 0;
}

int lsmtree::major_compact(){
    compaction *c = versions_->plan_compact();
    if(c==nullptr){ //do nothing
        compacting_ = false;
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
            }
        }
        int destlevel = c->level();
        sstable *sst = create_sst(destlevel, versions_->next_fnumber());
        edit.add(destlevel, sst);
        make_heap(vec.begin(), vec.end(), basetable::compare);
        while(!vec.empty()){
            basetable::iterator it = vec.front();
            pop_heap(vec.begin(), vec.end());
            kvtuple t = *it;
            it.next();
            if(it.valid()){
                push_heap(vec.begin(), vec.end());
            }else{
                vec.pop_back(); //remove iterator
            }
            if(sst->put(t.seqno, std::string(t.ckey), std::string(t.cval), t.flag)==ERROR_SPACE_NOT_ENOUGH){
                sst = create_sst(destlevel, versions_->next_fnumber());
                edit.add(destlevel, sst);
                sst->put(t.seqno, std::string(t.ckey), std::string(t.cval), t.flag);
            }
        }
    }
    versions_->apply(&edit);
    versions_->current()->calculate();

    assert(compacting_==true);
    compacting_ = false;

    schedule_compaction();
    return 0;
}

snapshot * lsmtree::get_snapshot(){
    return snapshots_.create(versions_->last_sequence());
}

int lsmtree::release_snapshot(snapshot *snap){
    return snapshots_.destroy(snap);
}
