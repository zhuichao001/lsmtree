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

    versions = new versionset;
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
    int seqno = (opt.snap==nullptr)? versions->last_sequence():opt.snap->sequence();
    mutab->ref();
    if(immutab!=nullptr){
        mutab->ref();
    }

    if(mutab->get(seqno, key, val)==0){
        return 0;
    }
    mutab->unref();

    if(immutab!=nullptr){ 
        if(immutab->get(seqno, key, val)==0){
            return 0;
        }
        immutab->unref();
    }

    version *cur = versions->current();
    return cur->get(seqno, key, val);
}

void lsmtree::schedule_compaction(){
    if(immutab!=nullptr){
        backstage.post([this]{this->minor_compact();});
        return;
    }

    //TODO: deal manual compaction

    if(versions->need_compact()){
        backstage.post([this]{this->major_compact();});
        return;
    }
}

int lsmtree::ensure_space(){
    if(mutab->size() < MAX_MEMTAB_SIZE){
        return 0;
    }

    std::unique_lock<std::mutex> lock{mutex};
    if (immutab!=nullptr) {
        level0_cv.wait(lock);
    } else if(mutab->size() >= SST_LIMIT){
        //TODO log file
        immutab = mutab;
        mutab = new memtable;
        mutab->ref();
        schedule_compaction();
    }
    mutex.unlock();
    return 0;
}

int lsmtree::put(const woptions &opt, const std::string &key, const std::string &val){
    if(ensure_space()){
        fprintf(stderr, "shift space error\n");
        return -1;
    }

    int seqno = versions->add_sequence(1);
    if(mutab->put(seqno, key, val)<0){ //TODO: USE Writebatch
        return -1;
    }

    return 0;
}

int lsmtree::del(const woptions &opt, const std::string &key){
    if(ensure_space()){
        fprintf(stderr, "shift space error\n");
        return -1;
    }

    int seqno = versions->add_sequence(1);
    if(mutab->del(seqno, key)<0){
        return -1;
    }

    return 0;
}

int lsmtree::minor_compact(){
    if(immutab==nullptr){
        fprintf(stderr, "error, when lsmtree::minor_compact immutab is null");
        return -1;
    }

    primarysst *sst = create_primarysst(versions->next_fnumber());
    immutab->scan(versions->last_sequence(), [=](const uint64_t seqno, const std::string &key, const std::string &val, int flag) ->int {
        sst->put(seqno, key, val, flag);
        return 0;
    });

    versionedit edit;
    edit.add(1, sst);

    version *cur = versions->current();
    cur->ref();
    versions->apply(&edit);
    cur->unref();

    {
        pthread_rwlock_wrlock(&lock);
        delete immutab;
        immutab = nullptr;
        pthread_rwlock_unlock(&lock);
    }

    return 0;
}

/*
int lsmtree::select_overlap(const int ln, std::vector<basetable*> &from, std::vector<basetable*> &to){
    int n = 0;
    for(int j=0; j<levels[ln].size(); ++j){
        basetable *sst = levels[ln][j];
        if(sst->state = basetable::COMPACTING){
            continue;
        }

        for(int i=0; i<from.size(); ++i){
            if(from[i]->state = basetable::COMPACTING){
                continue;
            }
            if(sst->overlap(from[i])){
                sst->state = basetable::COMPACTING;
                to.push_back(sst);
                ++n;
            }
        }
    }
    return n;
}
*/

int lsmtree::major_compact(){
    compaction *c = versions->plan_compact();
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
            }
        }
        int destlevel = c->level();
        sstable *sst = create_sst(destlevel, ++sstnumber);
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
                sst = create_sst(destlevel, ++sstnumber);
                edit.add(destlevel, sst);
                sst->put(t.seqno, std::string(t.ckey), std::string(t.cval), t.flag);
            }
        }
    }
    versions->apply(&edit);
    versions->current()->calculate();
    schedule_compaction();
    return 0;
}

snapshot * lsmtree::get_snapshot(){
    return snapshots.create(versions->last_sequence());
}

int lsmtree::release_snapshot(snapshot *snap){
    return snapshots.destroy(snap);
}
