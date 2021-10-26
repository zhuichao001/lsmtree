all:
	g++ -o run skiplist.cpp lsmtree.cpp main.cpp -lpthread
clean:
	rm -f run
