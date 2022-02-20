#ifndef _LSMTREE_WBATCH_H_
#define _LSMTREE_WBATCH_H_

#include <string>
#include <tuple>

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

    int scan(std::function<int(const char*, const char*, int)> func) const {
        for(int i=0; i<rows.size(); ++i){
            const char *key = std::get<0>(rows[i]).c_str();
            const char *val = std::get<1>(rows[i]).c_str();
            const int flag = std::get<2>(rows[i]);
            int err = func(key, val, flag);
            if(err<0){
                fprintf(stderr, "failed scan writebatch, %s:%s %d\n", key, val, flag);
                return -1;
            }
        }
        return 0;
    }

private:
    std::vector<kv_t> rows;
};

#endif
