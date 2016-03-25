#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=5.6.28

tsuite_wget 32182980 4bc8fde6d04fb7104df1ba8a4025b156 \
    217cd96921abdd709b9b4ff3ce2af4cbd237de43679cf19385d19df03a037b21 \
    http://downloads.mysql.com/archives/get/file/mysql-$V.tar.gz

tsuite_decompress mysql-$V.tar.gz | tar fx -
cd mysql-$V
cmake .
make
cd mysql-test

hasprog prlimit && sudo prlimit --pid $$ --nproc=5000 || :

./mysql-test-run --big-test --skip-test=federated
