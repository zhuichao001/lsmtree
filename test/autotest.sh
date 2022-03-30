#!/bin/bash
if [ ! -e ./run_test_default ]; then
    make
fi

j=$1
for ((i=0; i<j; ++i))
do
    echo "test NO-$i"
    rm -rf data out
    ./run_test_default &>out
    if [ $? -ne 0 ]; then
        exit -1
    fi
done
