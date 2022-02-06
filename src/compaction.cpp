#include "compaction.h"
#include "version.h"

void compaction::settle_inputs(){
    if (level>=MAX_LEVELS-1) {
        return;
    }

    std::string start = inputs[0][0]->smallest;
    std::string end = inputs[0][0]->largest;

    if (level==0) {
        for(int i=0; i<ver->ssts[0].size(); ++i){
            if(ver->ssts[0][i]->overlap(start, end)){
                inputs[1].push_back(ver->ssts[0][i]);
            }
        }
    } else {
        //TODO:speed up
        for (int delta=1; delta>=0; --delta) {
            for (int i=0; i<ver->ssts[level+delta].size(); ++i) {
                basetable *t = ver->ssts[level+delta][i];
                if (delta==0 && t==inputs[0][0]) {
                    continue;
                }
                if (!t->overlap(start, end)) {
                    continue;
                }
                inputs[delta].push_back(t);
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
