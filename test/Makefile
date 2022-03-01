all:
	g++ -g -o run_test_default \
		../src/encode.cpp \
		../src/hash.cpp \
		../src/fio.cpp \
		../src/skiplist.cpp \
		../src/primarysst.cpp \
		../src/sstable.cpp \
		../src/compaction.cpp \
		../src/version.cpp \
		../src/lsmtree.cpp \
		./test_default.cpp \
		-lpthread \
		-std=c++14 \
		-I../src 
kvtuple:
	g++ -g -o run_test_kvtuple \
		test_kvtuple.cpp \
		-I../src
primarysst:
	g++ -g -o run_test_primarysst \
		test_primarysst.cpp \
		../src/encode.cpp \
		../src/hash.cpp \
		../src/fio.cpp \
		../src/primarysst.cpp \
		-I../src
sstable:
	g++ -g -o run_test_sstable \
		test_sstable.cpp  \
		../src/encode.cpp \
		../src/hash.cpp \
		../src/fio.cpp \
		../src/sstable.cpp \
		-I../src
memtable:
	g++ -g -o run_test_memtable \
		test_memtable.cpp \
		../src/skiplist.cpp \
		-I../src
compact:
	g++ -g -o run_test_compact \
		test_compact.cpp  \
		../src/version.cpp  \
		../src/sstable.cpp \
		../src/primarysst.cpp \
		../src/hash.cpp  \
		../src/fio.cpp \
		../src/encode.cpp  \
		../src/compaction.cpp \
		-I../src
snapshot:
	g++ -g -o run_test_snapshot \
		test_snapshot.cpp \
		../src/version.cpp \
		../src/sstable.cpp \
		../src/primarysst.cpp \
		../src/hash.cpp \
		../src/fio.cpp \
		../src/encode.cpp \
		../src/compaction.cpp \
		../src/lsmtree.cpp \
		../src/skiplist.cpp \
		-lpthread \
		-std=c++14 \
		-I../src
update:
	g++ -g -o run_test_update \
		test_update.cpp \
		../src/version.cpp \
		../src/sstable.cpp \
		../src/primarysst.cpp \
		../src/hash.cpp \
		../src/fio.cpp \
		../src/encode.cpp \
		../src/compaction.cpp \
		../src/lsmtree.cpp \
		../src/skiplist.cpp \
		-lpthread \
		-std=c++14 \
		-I../src
cache:
	g++ -g -o run_test_cache \
		test_cache.cpp  \
		../src/encode.cpp \
		../src/hash.cpp \
		../src/fio.cpp \
		../src/sstable.cpp \
		-I../src
manifest:
	 g++ -g -o run_test_manifest \
		 test_manifest.cpp \
		 ../src/fio.cpp \
		 -I../src
clean:
	rm -rf run* data/

