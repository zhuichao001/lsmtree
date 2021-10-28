all:
	g++ -o run encode.cpp hash.cpp fio.cpp skiplist.cpp lsmtree.cpp main.cpp -lpthread -std=c++14
clean:
	rm -f run
