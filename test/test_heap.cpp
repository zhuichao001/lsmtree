#include<algorithm>
#include<vector>
#include<stdio.h>
#include<stdlib.h>

using namespace std;

bool compare(const vector<int>::iterator &a, const vector<int>::iterator &b){
    return *a > *b;
}

int test(){
    vector<vector<int>::iterator> vec;

    vector<int> va {1, 3, 5 ,7};
    vector<int> vb {2, 4, 6 ,8};

    vec.push_back(va.begin());
    vec.push_back(vb.begin());

    make_heap(vec.begin(), vec.end(), compare);
    while(!vec.empty()){
        auto it = vec.front();
        int d = *it;
        fprintf(stderr, "%d \n", d);
        pop_heap(vec.begin(), vec.end());
        vec.pop_back();
        ++it;
        if(d<=6){
            vec.push_back(it);
            push_heap(vec.begin(), vec.end(), compare);
        }
        //sort_heap(vec.begin(), vec.end(), compare);
    }

    return 0;
}

int main(){
    test();
    return 0;
}

