#include "lsmtree.h"
#include "sstable.h"
#include<algorithm>

const int TIER_SST_COUNT(int level){
    return TIER_PRI_COUNT * pow(10, level);
}

int lsmtree::open(const char *basedir){
    sprintf(pripath, "%s/pri/\0", basedir);
    sprintf(sstpath, "%s/sst/\0", basedir);
    if(!exist(basedir)){
        mkdir(pripath);
        mkdir(sstpath);
        return 0;
    }

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
    return 0;
}

int lsmtree::get(const std::string &key, std::string &val){
    if(mutab->get(key, val)==0){
        return 0;
    }

    if(immutab!=nullptr && immutab->get(key, val)==0){
        return 0;
    }

    for(int i=0; i<MAX_LEVELS; ++i){
        std::vector<basetable*>::iterator it = std::lower_bound(levels[i].begin(), levels[i].end(), key, 
                [](const basetable *b, const std::string &k) { return b->smallest < k; });
        if(it==levels[i].end()){
            continue;
        }
        if((*it)->get(key,val)==0){
            return 0;
        }
    }
    return -1;
}

void lsmtree::swapmutab(){
    if(immutab!=nullptr){
        tamp->wait();
    }
    std::lock_guard<std::mutex> lock(mux);
    memtable *rabbish = immutab;  //TODO if exist other readers
    delete rabbish;

    immutab = mutab;
    tamp->notify();

    mutab = new memtable;
}

int lsmtree::put(const std::string &key, const std::string &val){
    ++verno;
    if(mutab->put(key, val)<0){ //TODO pass verno
        return -1;
    }

    if(mutab->size() >= MUTABLE_LIMIT){
        swapmutab();
    }
    return 0;
}

int lsmtree::del(const std::string &key){
    ++verno;
    if(mutab->del(key)<0){ //TODO pass verno
        return -1;
    }

    if(mutab->size() >= MUTABLE_LIMIT){
        swapmutab();
    }
    return 0;
}

int lsmtree::minor_compact(){
    primarysst *sst = create_primarysst(++sstnumber);

    immutab->scan([=](const std::string &key, const std::string &val, int flag) ->int {
        sst->put(key, val, flag);
        return 0;
    });

    {
        std::lock_guard<std::mutex> lock(mux);
        //TODO:calculate level-n, push as down as possible
        levels[0].push_back(sst);
        delete immutab;
        immutab = nullptr;
    }
    return 0;
}

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

int lsmtree::major_compact(int ln){
    std::vector<basetable*> inputs[2];

    levels[ln][0]->state = basetable::COMPACTING;
    inputs[0].push_back(levels[0][0]);
    int lev = 1;
    while(select_overlap(lev, inputs[1-lev], inputs[lev])>0){
        lev = 1-lev;
    }

    std::vector<basetable::iterator> vec; //collect all iterators
    for(int i=0; i<2; ++i){
        for(int j=0; j<inputs[i].size(); ++j){
            basetable::iterator it = inputs[i][j]->begin();
            if(!it.valid()){
                continue;
            }
            vec.push_back(it);
        }
    }

    int destlevel = ln+1;
    sstable *sst = create_sst(destlevel, ++sstnumber);
    levels[destlevel].push_back(sst);

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

        if(sst->put(std::string(t.ckey), std::string(t.cval), t.flag)==ERROR_SPACE_NOT_ENOUGH){
            sst = create_sst(destlevel, ++sstnumber);
            levels[destlevel].push_back(sst);
            sst->put(std::string(t.ckey), std::string(t.cval), t.flag);
        }
    }

    sort_sst(destlevel);
    //TODO: how to release the source ssts
    return 0;
}

//transfer hot-data in immutable down to sst
int lsmtree::sweep(){
    minor_compact();

    int compact_levels = 0;   
    for(int i=0; i<MAX_LEVELS; ++i){
        if(levels[i].size() >= TIER_PRI_COUNT){ //TODO cal condition
            major_compact(i);
            if(++compact_levels == MAX_COMPACT_LEVELS){
                break;
            }
        }
    }
    return 0;
}

snapshot * lsmtree::create_snapshot(){
    return nullptr;
}

int lsmtree::release_snapshot(){
    return 0;
}
