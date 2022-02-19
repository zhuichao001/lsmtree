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

    vector<int> va {1, 2, 3, 4};
    vector<int> vb {5, 6, 7, 8};
    vector<int> vc {9, 10, 11, 12};

    vec.push_back(va.begin());
    vec.push_back(vb.begin());
    vec.push_back(vc.begin());

    make_heap(vec.begin(), vec.end(), compare);
    while(!vec.empty()){
        int d = *vec[0];
        fprintf(stderr, "%d \n", d);
        pop_heap(vec.begin(), vec.end(), compare);
        ++vec[vec.size()-1];
        if(d%4!=0){
            push_heap(vec.begin(), vec.end(), compare);
        }else{
            vec.pop_back();
        }
    }

    return 0;
}

int main(){
    test();
    return 0;
}

