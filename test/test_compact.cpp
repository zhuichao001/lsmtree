#include<iostream>
#include<string>
#include "sstable.h"
#include "version.h"

sstable *gen_sst(int start, int end){
    sstable * sst = new sstable(1, 1001);
    char path[128];
    sprintf(path, "./sst-%d-%d.dat", start, end);
    sst->create(path);
    
    for(int i=start; i<=end; ++i){
        char k[128];
        char v[128];
        sprintf(k, "key_%d", i);
        sprintf(v, "val_%d", i);
        
        std::string key=k;
        std::string val=v;
        int err = sst->put(i, key, val);
        if(err!=0){
            fprintf(stderr, "err:%d\n", err);
        }
    }
    return sst;
}

void test(){
    sstable *ta = gen_sst(1,3);
    ta->print(999999);
    fprintf(stderr, "--------------\n");
    sstable *tb = gen_sst(4,6);
    sstable *tc = gen_sst(7,9);

    tb->print(999999);
    fprintf(stderr, "--------------\n");
    tc->print(999999);
    fprintf(stderr, "--------------\n");

    std::vector<basetable::iterator> vec{ta->begin(), tb->begin(), tc->begin()};

    int fn=1000;
    int destlevel = 1;
    versionedit edit;
    {
        sstable *sst = create_sst(destlevel, ++fn);
        edit.add(destlevel, sst);
        make_heap(vec.begin(), vec.end(), basetable::compare);
        while(!vec.empty()){
            basetable::iterator it = vec.front();
            pop_heap(vec.begin(), vec.end(), basetable::compare);
            vec.pop_back(); //remove iterator

            kvtuple t;
            it.parse(t);
            it.next();
            if(it.valid()){
                vec.push_back(it);
                push_heap(vec.begin(), vec.end(), basetable::compare);
            }
            fprintf(stderr, "  : %s %s\n", t.ckey, t.cval);

            if(sst->put(t.seqno, std::string(t.ckey), std::string(t.cval), t.flag)==ERROR_SPACE_NOT_ENOUGH){
                fprintf(stderr, "major compact into sst-%d range:[%s, %s]\n", sst->file_number, sst->smallest.c_str(), sst->largest.c_str());
                sst = create_sst(destlevel, ++fn);
                edit.add(destlevel, sst);
                sst->put(t.seqno, std::string(t.ckey), std::string(t.cval), t.flag);
            }
        }
        fprintf(stderr, "major compact into sst-%d range:[%s, %s]\n", sst->file_number, sst->smallest.c_str(), sst->largest.c_str());
    }

    /*
    fprntf(stderr, "---------------------\n");
    for(int i=i; i<edit.delfiles.size(); ++i){
        fprintf(stderr, "  del sst-%d\n", edit.delfiles[i]->file_number);
    }
    */
    fprintf(stderr, "+++++++++++++++++++++\n");
    for(int i=i; i<7; ++i){
        for(int j=0; j<edit.addfiles[i].size(); ++j){
            fprintf(stderr, "  add level-%d sst-%d\n", j, edit.addfiles[i][j]->file_number);
            edit.addfiles[i][j]->print(999999999);
        }
    }
}

int main(){
    test();
    return 0;
}
