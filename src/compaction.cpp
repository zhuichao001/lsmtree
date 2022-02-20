#include "compaction.h"
#include "version.h"

void compaction::settle_inputs(version *ver){
    if (level_>=MAX_LEVELS-1) {
        return;
    }

    std::string start = inputs_[0][0]->smallest;
    std::string end = inputs_[0][0]->largest;

    if (level_==0) {
        for(int i=0; i<ver->ssts[0].size(); ++i){
            if(ver->ssts[0][i]==inputs_[0][0]){
                continue;
            }
            if(ver->ssts[0][i]->overlap(start, end)){
                inputs_[1].push_back(ver->ssts[0][i]);
            }
        }
    } else {
        //TODO:speed up
        for (int delta=1; delta>=0; --delta) {
            for (int i=0; i<ver->ssts[level_+delta].size(); ++i) {
                basetable *t = ver->ssts[level_+delta][i];
                if (delta==0 && t==inputs_[0][0]) {
                    continue;
                }
                if (!t->overlap(start, end)) {
                    continue;
                }
                inputs_[delta].push_back(t);
                if (t->smallest<start) {
                    start = t->smallest;
                }
                if (t->largest<end) {
                    end = t->smallest;
                }
            }
        }
    }
}
