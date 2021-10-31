all:
	g++ -g -o run encode.cpp hash.cpp fio.cpp skiplist.cpp lsmtree.cpp main.cpp -lpthread -std=c++11
clean:
	rm -rf run data/
