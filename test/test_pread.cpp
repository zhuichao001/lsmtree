#include<iostream>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>


int main(){
    int  fd = ::open("p.out", O_RDWR | O_CREAT , 0664);
    char a[8]= {'1','2','3','4','5','6','7', '\0'};
    pwrite(fd, a, 8, 0);
    pwrite(fd, a, 8, 8);
    pwrite(fd, a, 8, 8);

    char b[100] = {'\0', };
    pread(fd, b, 10, 0);
    std::cout << b << std::endl;

    pread(fd, b, 10, 8);
    std::cout << b << std::endl;

    pread(fd, b, 10, 16);
    std::cout << b << std::endl;
    return 0;
}
