all:
	g++ -g -o run \
		../src/encode.cpp \
		../src/hash.cpp \
		../src/fio.cpp \
		../src/skiplist.cpp \
		../src/primarysst.cpp \
		../src/sstable.cpp \
		../src/lsmtree.cpp \
		./main.cpp \
		-lpthread \
		-std=c++11 \
		-I../src 
clean:
	rm -rf run data/
