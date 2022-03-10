#include <algorithm>
#include "lsmtree.h"
#include "sstable.h"


thread_pool_t backstage(1);
std::string basedir;

const int TIER_SST_COUNT(int level){
    return TIER_PRI_COUNT * pow(10, level);
}

int lsmtree::open(const options *opt, const char *dirpath){
    basedir = dirpath;
    char path[64];
    sprintf(path, "%s/sst/\0", dirpath);
    if(!exist(path)){
        mkdir(path);
    }

    wal::option walopt = {20971520, 1024};
    char logpath[64];
    sprintf(logpath, "%s/log/\0", basedir.c_str());
    wal_ = new wal::walog(walopt, logpath);

    recover();
    return 0;
}

int lsmtree::recover(){
    versions_.recover();

    int startidx = versions_.apply_logidx()+1; 
    int endidx = -1;
    wal_->truncatefront(startidx);
    wal_->lastindex(endidx);
    for(int idx=startidx; idx<=endidx; ++idx){
        std::string row;
        wal_->read(idx, row);
        int seqno=0;
        char kv[2][523];
        int flag=0;
        memset(kv, 0, sizeof(kv));
        sscanf(row.c_str(), "%d %s %s %d", &seqno, kv[0], kv[1], &flag);
        if(flag==FLAG_VAL){
            mutab_->put(idx, seqno, kv[0], kv[1]);
        }else{
            mutab_->del(idx, seqno, kv[0]);
        }
    }
    return 0;
}

int lsmtree::get(const roptions &opt, const std::string &key, std::string &val){
    int seqno = (opt.snap==nullptr)? versions_.last_sequence() : opt.snap->sequence();
    memtable *mtab = nullptr;
    memtable *imtab = nullptr;
    version *ver = nullptr;
    {
        std::unique_lock<std::mutex> lock{mutex_};
        mtab = mutab_;
        imtab = immutab_;
        ver = versions_.current();
        mutab_->ref();
        if(immutab_!=nullptr){
            immutab_->ref();
        }
        ver->ref();
    }

    {
        int err = mtab->get(seqno, key, val);
        mtab->unref();
        if(err==0){
            if(imtab!=nullptr){
                imtab->unref();
            }
            ver->unref();
            return 0;
        }
    }

    if(immutab_!=nullptr){
        int err = immutab_->get(seqno, key, val);
        immutab_->unref();
        if(err==0){
            ver->unref();
            return 0;
        }
    }

    {
        int err = ver->get(seqno, key, val);
        ver->unref();
        schedule_compaction();
        return err;
    }
}

void lsmtree::schedule_compaction(){
    if(compacting){
        return;
    }

    compacting = true;
    backstage.post([this]{
        {
            if(immutab_!=nullptr){
                this->minor_compact();
            } 
            bool compacted = false;
            if(versions_.need_compact()){
                compaction *c = versions_.plan_compact();
                if(c!=nullptr){ //do nothing
                    this->major_compact(c);
                    compacted = true;
                }
            }
            compacting = false;
            if(compacted){
                schedule_compaction();
            }
        }
    });
}

int lsmtree::sweep_space(){
    if(mutab_->size() < MAX_MEMTAB_SIZE){
        return 0;
    }

    std::unique_lock<std::mutex> lock{mutex_};
    if (immutab_!=nullptr) {
        fprintf(stderr, "level-0 wait\n");
        solid_cv_.wait(lock);
        return 0;
    } else {
        immutab_ = mutab_;
        mutab_ = new memtable;
        mutab_->ref();
        return 1;
    }
}

int lsmtree::put(const woptions &opt, const std::string &key, const std::string &val){
    wbatch wb;
    if(wb.put(key, val)<0){
        return -1;
    }
    return write(opt, wb);
}

int lsmtree::del(const woptions &opt, const std::string &key){
    wbatch wb;
    if(wb.del(key)<0){
        return -1;
    }
    return write(opt, wb);
}

int lsmtree::minor_compact(){
    if(immutab_==nullptr){
        return -1;
    }

    int persist_logidx = -1;
    versionedit edit;
    primarysst *sst = new primarysst(versions_.next_fnumber());
    sst->open();
    immutab_->scan(versions_.last_sequence(), [=, &edit, &sst, &persist_logidx](const int logidx, const uint64_t seqno, const std::string &key, const std::string &val, int flag) ->int {
        persist_logidx = logidx;
        if(sst->put(seqno, key, val, flag)==ERROR_SPACE_NOT_ENOUGH){
            fprintf(stderr, "minor compact into sst-%d range:[%s, %s]\n", sst->file_number, sst->smallest.c_str(), sst->largest.c_str());
            edit.add(0, sst);
            sst = new primarysst(versions_.next_fnumber());
            sst->open();
            sst->put(seqno, key, val, flag);
        }
        return 0;
    });

    fprintf(stderr, "minor compact into sst-%d range:[%s, %s]\n", sst->file_number, sst->smallest.c_str(), sst->largest.c_str());
    edit.add(0, sst);

    versions_.apply_logidx(persist_logidx);
    version *neo = versions_.apply(&edit);
    {
        std::unique_lock<std::mutex> lock{mutex_};
        immutab_->unref();
        immutab_ = nullptr;
    }
    versions_.appoint(neo);
    solid_cv_.notify_all();
    versions_.current()->calculate();
    return 0;
}

int lsmtree::major_compact(compaction* c){
    assert(c->inputs_[0].size()>0);
    versionedit edit;
    if(c->size()==1){
        //directly move to next level
        edit.remove(c->inputs_[0][0]);
        edit.add(c->level(), c->inputs_[0][0]);
    } else {
        std::vector<basetable::iterator> vec;
        for(int i=0; i<2; ++i){
            for(basetable *t : c->inputs_[i]){
                edit.remove(t);
                vec.push_back(t->begin());
                fprintf(stderr, "   ...major compact from input-%d level:%d  sst-%d <%s, %s>\n", i, t->level, t->file_number, t->smallest.c_str(), t->largest.c_str());
            }
        }
        if(vec.empty()){
            return 0;
        }

        const int destlevel = c->level(); //compact into next level
        sstable *sst = new sstable(destlevel, versions_.next_fnumber());
        sst->open();
        edit.add(destlevel, sst);

        int n = 0; //merge keys
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

            if(n++>0 && lastkey==t.ckey && t.seqno<snapshots_.smallest_sn()){
                continue;
            }

            lastkey = t.ckey;

            if(sst->put(t.seqno, std::string(t.ckey), std::string(t.cval), t.flag)==ERROR_SPACE_NOT_ENOUGH){
                fprintf(stderr, "    >>>major compact into level:%d sst-%d range:[%s, %s]\n", destlevel, sst->file_number, sst->smallest.c_str(), sst->largest.c_str());

                sst = new sstable(destlevel, versions_.next_fnumber());
                sst->open();
                edit.add(destlevel, sst);

                sst->put(t.seqno, std::string(t.ckey), std::string(t.cval), t.flag);
            }
        }
        fprintf(stderr, "    >>>major compact into level:%d sst-%d range:[%s, %s]\n", destlevel, sst->file_number, sst->smallest.c_str(), sst->largest.c_str());
    }

    version * neo = versions_.apply(&edit);

    {
        //std::unique_lock<std::mutex> lock{mutex_};
        versions_.appoint(neo);
    }

    versions_.current()->calculate();
    fprintf(stderr, "major compact DONE!!!\n");
    return 0;
}

int lsmtree::write(const woptions &opt, const wbatch &bat){
    int seqno = versions_.add_sequence(bat.size());
    bat.scan([this, seqno](const char *key, const char *val, const int flag)->int{
        if(sweep_space()==1){
            schedule_compaction();
        }

        char log[1024];
        if(flag==FLAG_VAL){
            sprintf(log, "%d %s %s %d\n\0", seqno, key, val, flag);
            wal_->write(++logidx_, log);
            return mutab_->put(logidx_, seqno, key, val);
        }else{
            sprintf(log, "%d %s * %d\n\0", seqno, key, flag);
            wal_->write(++logidx_, log);
            return mutab_->del(logidx_, seqno, key);
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
