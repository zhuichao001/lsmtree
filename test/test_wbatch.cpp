#include "./wbatch.h"

void test(){
    wbatch wb;
    wb.put("abc","abc");
    wb.del("de");

    std::string data;
    wb.to_string(data);

    wbatch wc;
    wc.from_string(data);
    wc.print();

}

int main(){
    test();
}
