#include "lsmtree.h"
#include "compaction.h"

extern std::string basedir ;

void test(){
    basetable *t1 = new sstable(3, 11251, "key_1964230", "key_198015");
    t1->open();
    t1->load();
    t1->ref();
    basetable *t2 = new sstable(2, 11240, "key_1967211", "key_1971890");
    t2->open();
    t2->load();
    t2->ref();
    compaction *c = new compaction(3);
    c->inputs_[0].push_back(t1);
    c->inputs_[1].push_back(t2);

    lsmtree db;
    db.major_compact(c);
}


int main(){
    basedir = "./data/";
    test();
    return 0;
}
