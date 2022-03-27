#include <map>


int main(){
    std::multimap<int, int> codemap; //hashcode => kvtuple(in order to speed up)
    for(int i=0; i<2000; ++i){
	    codemap.insert(std::make_pair(i, i));
    }

    bool same=false;
    for(int i=0; i<2000; ++i){
        auto pr = codemap.equal_range(i);
        for (auto iter = pr.first ; iter != pr.second; ++iter){
            const int v = iter->second;
            if(v==i){
                same = true;
                fprintf(stderr, "ok, i=%d\n", i);
            }
        }
	if(!same){
            fprintf(stderr, "not same, i=%d\n", i);
	}
    }

    return 0;
}
