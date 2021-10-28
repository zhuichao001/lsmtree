all:
	g++ -o run skiplist.cpp hash.cpp fio.cpp lsmtree.cpp main.cpp -lpthread -std=c++14
clean:
	rm -f run
