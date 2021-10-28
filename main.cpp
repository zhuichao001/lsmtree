#include <iostream>
#include "lsmtree.h"


int main(){
    lsmtree db;
    db.open("./data");
    db.put("a","b");

    std::string val;
    db.get("a",  val);
    std::cout<< "val:" << val <<std::endl;
    return 0;
}
