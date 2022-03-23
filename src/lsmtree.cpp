#include <algorithm>
#include "lsmtree.h"
#include "sstable.h"
#include "clock.h"


std::string basedir;

int lsmtree::open(const options *opt, const char *dirpath){
    basedir = dirpath;
    char path[64];
    sprintf(path, "%s/sst/\0", dirpath);
    if(!exist(path)){
        mkdir(path);
    }
    versions_.recover();

    wal::option walopt = {20971520, 1024};
    char logpath[64];
    sprintf(logpath, "%s/log/\0", basedir.c_str());
    wal_ = new wal::walog(walopt, logpath);
    redolog();

    flusher_ = new std::thread(std::bind(&lsmtree::schedule_flush, this));
    return 0;
}

int lsmtree::redolog(){
    int startidx = versions_.apply_logidx()+1; 
    int endidx = -1;
    wal_->truncatefront(startidx);
    wal_->lastindex(endidx);
    for(int idx=startidx; idx<=endidx; ++idx){
        std::string row;
        if(wal_->read(idx, row)<0){
            fprintf(stderr, "error: failed when redo log, idx=%d\n", idx);
            return -1;
        }
        int seqno=0;
        char kv[2][512];
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
    memtable *imtab[MAX_IMMUTAB_SIZE];
    int tabnum = 0;

    {
        std::unique_lock<std::mutex> lock{mutex_};
        mtab = mutab_;
        mutab_->ref();
        tabnum = imsize_;
        for(int i=0; i<tabnum; ++i){
            imtab[i] = immutab_[i];
            imtab[i]->ref();
        }
    }

    {
        int err = mtab->get(seqno, key, val);
        mtab->unref();
        if(err==0){
            for(int i=0; i<tabnum; ++i){
                imtab[i]->unref();
            }
            return 0;
        }
    }

    {
        int err = -1;
        for(int i=0; i<tabnum; ++i){
            if(err<0){
                err = imtab[i]->get(seqno, key, val);
            }
            imtab[i]->unref();
        }
        if(err==0){
            return 0;
        }
    }

    {
        version *ver = ver = versions_.curversion();
        int err = ver->get(seqno, key, val);
        ver->unref();
        schedule_compaction();
        return err;
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

int lsmtree::write(const woptions &opt, const wbatch &bat){
    int seqno = versions_.add_sequence(bat.size());
    bat.scan([this, seqno](const char *key, const char *val, const int flag)->int{
        make_space(); //transfer full memtable to immutable

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

void lsmtree::schedule_flush(){
    while(running_){
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 2;   //tomeout 2 seconds
        if(sem_timedwait(&sem_occu_, &ts)!=0){
            continue;
        }
        int tabnum = 0;
        {
            std::unique_lock<std::mutex> lock{mutex_};
            tabnum = imsize_;
	    if(tabnum>MAX_FLUSH_SIZE){
            	tabnum = MAX_FLUSH_SIZE;
	    }
        }
        if(tabnum<2){
            continue;
        }

	long start = get_time_usec();
        minor_compact(tabnum);
	fprintf(stderr, "minor cost: %d, flush num:%d\n", get_time_usec()-start, tabnum);
        for(int i=0; i<tabnum; ++i){
            sem_post(&sem_free_);
        }

        versions_.current()->calculate();
        schedule_compaction();
    }
}

void lsmtree::schedule_compaction(){
    if(compacting_){
        return;
    }

    compacting_ = true;
    backstage_.post([this]{
        bool compacted = false;
        if(versions_.need_compact()){
            compaction *c = nullptr;
            {
                std::unique_lock<std::mutex> lock{sstmutex_};
                c = versions_.plan_compact(versions_.current());
            }
            if(c!=nullptr){ //do nothing
		long start = get_time_usec();
                this->major_compact(c);
		fprintf(stderr, "major cost: %d\n", get_time_usec()-start);
                compacted = true;
            }
        }
        compacting_ = false;
        if(compacted){
            versions_.current()->calculate();
            schedule_compaction();
        }
    });
}

int lsmtree::make_space(){
    if(mutab_->size() < MAX_MEMTAB_SIZE){
        return 0;
    }

    sem_wait(&sem_free_);
    {
        std::unique_lock<std::mutex> lock{mutex_};
        immutab_[imsize_++] = mutab_;
        mutab_ = new memtable;
        mutab_->ref();
    }
    sem_post(&sem_occu_);
    return 0;
}

int lsmtree::minor_compact(const int tabnum){
    assert(tabnum>0);
    std::vector<memtable::iterator> vec;
    for(int i=0; i<tabnum; ++i){
        vec.push_back(immutab_[i]->begin());
    }

    int n = 0; //merge keys
    std::string lastkey;
    int persist_logidx = -1;
    versionedit edit;

    primarysst *sst = new primarysst(versions_.next_fnumber());
    sst->open();
    edit.add(0, sst);

    make_heap(vec.begin(), vec.end(), memtable::compare_gt);
    while(!vec.empty()){
        memtable::iterator it = vec.front();
        pop_heap(vec.begin(), vec.end(), memtable::compare_gt);
        vec.pop_back(); //remove iterator
        if(!it.valid()){
            fprintf(stderr, "error: pop invalid memtable::iterator\n");
            continue;
        }

        onval *t = it.val();
        it.next();
        if(it.valid()){
            vec.push_back(it);
            push_heap(vec.begin(), vec.end(), memtable::compare_gt);
        }

        //merge same key
        if(n++>0 && lastkey==t->key && t->seqno<snapshots_.smallest_sn()){
            continue;
        }

        if(persist_logidx < t->logidx){
            persist_logidx = t->logidx;
        }
        if(sst->put(t->seqno, t->key, t->val, t->flag)==ERROR_SPACE_NOT_ENOUGH){
	    sst->close();
            fprintf(stderr, "    >>>minor compact into sst-%d range:[%s, %s]\n", 
			    sst->file_number, sst->smallest.c_str(), sst->largest.c_str());

            sst = new primarysst(versions_.next_fnumber());
            sst->open();
            edit.add(0, sst);

            sst->put(t->seqno, t->key, t->val, t->flag);
        }
        lastkey = t->key;
    }
    fprintf(stderr, "    >>>minor compact into sst-%d range:[%s, %s]\n", 
		    sst->file_number, sst->smallest.c_str(), sst->largest.c_str());

    {
        std::unique_lock<std::mutex> lock{sstmutex_};
        version *neo = versions_.apply(&edit); 
        versions_.appoint(neo);
    }

    versions_.persist(versions_.current());
    versions_.apply_logidx(persist_logidx);

    {
        std::unique_lock<std::mutex> lock{mutex_};
        for(int i=0; i<tabnum; ++i){
            immutab_[i]->unref();
        }
        for(int i=0; i<imsize_-tabnum; ++i){
            immutab_[i] = immutab_[tabnum+i];
        }
        imsize_ -= tabnum;
    }
    fprintf(stderr, "MINOR COMPACT DONE!!!\n");
    return 0;
}

int lsmtree::major_compact(compaction* c){
    assert(c->size()>0);

    versionedit edit;
    if(c->size()==1){
        //directly move to next level
        edit.remove(c->inputs()[0]);
        edit.add(c->level(), c->inputs()[0]);
    } else {
        std::vector<basetable::iterator> vec;
        for(basetable *t : c->inputs()){
            versions_.cachein(t);
            edit.remove(t);
            vec.push_back(t->begin());
            fprintf(stderr, "   ...major compact from level:%d  sst-%d <%s, %s>\n", t->level, t->file_number, t->smallest.c_str(), t->largest.c_str());
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

            basetable *t = it.belong();
            versions_.cachein(t);

            kvtuple e;
            it.parse(e);

            it.next();
            if(it.valid()){
                vec.push_back(it);
                push_heap(vec.begin(), vec.end(), basetable::compare_gt);
            }

            if(n++>0 && lastkey==e.ckey && e.seqno<snapshots_.smallest_sn()){
                continue;
            }

            if(sst->put(e.seqno, std::string(e.ckey), std::string(e.cval), e.flag)==ERROR_SPACE_NOT_ENOUGH){
                fprintf(stderr, "    >>>major compact into level:%d sst-%d range:[%s, %s]\n", destlevel, sst->file_number, sst->smallest.c_str(), sst->largest.c_str());

                sst = new sstable(destlevel, versions_.next_fnumber());
                sst->open();
                edit.add(destlevel, sst);

                sst->put(e.seqno, std::string(e.ckey), std::string(e.cval), e.flag);
            }
            lastkey = e.ckey;
        }
        fprintf(stderr, "    >>>major compact into level:%d sst-%d range:[%s, %s]\n", destlevel, sst->file_number, sst->smallest.c_str(), sst->largest.c_str());
    }

    {
        std::unique_lock<std::mutex> lock{sstmutex_};
        version *neo = versions_.apply(&edit);
        versions_.appoint(neo);
    }

    versions_.persist(versions_.current());
    fprintf(stderr, "MAJOR COMPACT DONE!!!\n");
    return 0;
}

snapshot * lsmtree::create_snapshot(){
    return snapshots_.create(versions_.last_sequence());
}

int lsmtree::release_snapshot(snapshot *snap){
    return snapshots_.destroy(snap);
}
