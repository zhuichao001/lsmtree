#ifndef _LSMTREE_WBATCH_H_
#define _LSMTREE_WBATCH_H_

#include <memory.h>
#include <string>
#include <tuple>
#include <vector>
#include <functional>
#include "type.h"

typedef std::tuple<std::string, std::string, int> kv_t;

class wbatch{
public:
    int put(const std::string &key, const std::string &val){
        rows.push_back(std::make_tuple(key, val, FLAG_VAL));
        return 0;
    }

    int del(const std::string &key){
        rows.push_back(std::make_tuple(key, "*", FLAG_DEL));
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

    int size() const {
        return rows.size();
    }

    void print(){
        for(kv_t r : rows){
            const char *key = std::get<0>(r).c_str();
            const char *val = std::get<1>(r).c_str();
            const int flag = std::get<2>(r);
            fprintf(stderr, "row= %s:%s %d\n", key, val, flag);
        }
    }

    //int to_string(std::string &data)const{
    //    for(kv_t r : rows){
    //        char tmp[1024];
    //        sprintf(tmp, "%s %s %d\n", std::get<0>(r).c_str(), std::get<1>(r).c_str(), std::get<2>(r));
    //        data += tmp;
    //    }
    //    return 0;
    //}

    //int from_string(const std::string &data){
    //    const char *SEPRATOR = "\n";
    //    char *token = strtok(const_cast<char*>(data.c_str()), SEPRATOR);
    //    while(token!=nullptr){
    //        kvtuple t;
    //        const int len = strlen(token);
    //        char ckey[len];
    //        char cval[len];
    //        int flag = 0;
    //        memset(ckey, 0, sizeof(ckey));
    //        memset(cval, 0, sizeof(cval));
    //        sscanf(token, "%s %s %d\n", ckey, cval, &flag);
    //        rows.push_back(std::make_tuple(std::string(ckey), std::string(cval), flag));

    //        token = strtok(nullptr, SEPRATOR);
    //    }
    //    return 0;
    //}

private:
    std::vector<kv_t> rows;
};

#endif
