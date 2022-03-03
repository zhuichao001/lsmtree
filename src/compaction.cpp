#include "compaction.h"
#include "version.h"

void compaction::settle_inputs(version *ver){
    assert(level_>=1);
    assert(level_<MAX_LEVELS);

    std::string start = inputs_[0][0]->smallest;
    std::string end = inputs_[0][0]->largest;

    std::set<basetable *> unique;
    unique.insert(inputs_[0][0]);

    if (level_==1) {
        for(int i=0; i<ver->ssts[0].size(); ++i){
            basetable *t = ver->ssts[0][i];
            if(unique.count(t)!=0){
                continue;
            }
            if(t->overlap(start, end)){
                inputs_[1].push_back(t);
            }
        }
    } else {
        fprintf(stderr, " settle begin level:%d sst-%d <%s, %s>\n", 
                inputs_[0][0]->file_number, inputs_[0][0]->level, start.c_str(), end.c_str());

        for (int delta=1; ; delta=1-delta) {
            int level = level_-delta;
            int affected = 0;
            for (int j=0; j<ver->ssts[level].size(); ++j) {
                basetable *t = ver->ssts[level][j];
                if (unique.count(t)!=0) {
                    continue;
                }
                if (!t->overlap(start, end)) {
                    continue;
                }
                inputs_[delta].push_back(t);
                unique.insert(t);
                ++affected;

                if(t->smallest < start){
                    start = t->smallest;
                }
                if(t->largest > end){
                    end = t->largest;
                }

                fprintf(stderr, "   settle add level:%d <%s, %s> => <%s, %s>\n", 
                        level, t->smallest.c_str(), t->largest.c_str(), start.c_str(), end.c_str());
            }
            if (affected==0) {
                break;
            }
        }
    }
}
