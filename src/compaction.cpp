#include "compaction.h"
#include "version.h"

void compaction::settle(){
    assert(level_>0 && level_<MAX_LEVELS);

    inputs_.push_back(from_);
    fprintf(stderr, "     |plan compaction| start:%s, end:%s\n", start_.c_str(), end_.c_str());

    //TODO optimize by binary search
    for (basetable *t : ver_->ssts[level_]) {
        if (!t->overlap(start_, end_)) {
            continue;
        }

        inputs_.push_back(t);
        fprintf(stderr, "     |plan compaction| add level:%d sst-%d <%s, %s>\n", 
                t->level, t->file_number, t->smallest.c_str(), t->largest.c_str());

        if (t->smallest < start_) {
            start_ = t->smallest;
        }
        if (t->largest > end_) {
            end_ = t->largest;
        }
    }

    for (basetable *t : ver_->ssts[level_-1]) {
        if (t!=inputs_[0] && t->containedby(start_, end_)) {
            fprintf(stderr, "     |post plan compaction| add level:%d sst-%d <%s, %s>\n", 
                    t->level, t->file_number, t->smallest.c_str(), t->largest.c_str());
            inputs_.push_back(t);
        }
    }
}
