g++ -o run_test_kvtuple test_kvtuple.cpp -I../src
g++ -o run_test_primarysst test_primarysst.cpp ../src/encode.cpp ../src/hash.cpp ../src/fio.cpp ../src/primarysst.cpp -I../src
g++ -o run_test_sstable test_sstable.cpp  ../src/encode.cpp ../src/hash.cpp ../src/fio.cpp ../src/sstable.cpp -I../src
g++ -o run_test_memtable test_memtable.cpp ../src/skiplist.cpp -I../src