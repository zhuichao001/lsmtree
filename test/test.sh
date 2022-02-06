g++ test_kvtuple.cpp -I../src
g++ test_primarysst.cpp ../src/encode.cpp ../src/hash.cpp ../src/fio.cpp ../src/primarysst.cpp -I../src
g++ test_sstable.cpp ../src/encode.cpp ../src/hash.cpp ../src/fio.cpp ../src/sstable.cpp -I../src
g++ test_compaction.cpp ../src/encode.cpp ../src/hash.cpp ../src/fio.cpp ../src/primarysst.cpp ../src/sstable.cpp ../src/compaction.cpp ../src/version.cpp -I../src
