#ifndef _LSMTREE_WBATCH_H_
#define _LSMTREE_WBATCH_H_


typedef std::tuple<std::string, std::string, int> kv_t;

class wbatch{
public:
    int put(const std::string &key, const std::string &val){
        rows.push_back(std::make_tuple(key, val, FLAG_VAL));
        return 0;
    }

    int del(const std::string &key){
        rows.push_back(std::make_tuple(key, "", FLAG_DEL));
        return 0;
    }
private:
    std::vector<kv_t> rows;
};

#endif
