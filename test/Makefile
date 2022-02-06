all:
	g++ -g -o run \
		../src/encode.cpp \
		../src/hash.cpp \
		../src/fio.cpp \
		../src/skiplist.cpp \
		../src/primarysst.cpp \
		../src/sstable.cpp \
		../src/compaction.cpp \
		../src/version.cpp \
		../src/lsmtree.cpp \
		./main.cpp \
		-lpthread \
		-std=c++14 \
		-I../src 
clean:
	rm -rf run data/
