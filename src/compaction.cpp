#include "compaction.h"
#include "version.h"

void compaction::settle_inputs(version *ver){
    assert(level_>=1 && level_<MAX_LEVELS);
    std::string start = inputs_[0][0]->smallest;
    std::string end = inputs_[0][0]->largest;

    std::set<basetable *> unique;
    unique.insert(inputs_[0][0]);

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
        }
        if (affected==0) {
            break;
        }
    }
}
