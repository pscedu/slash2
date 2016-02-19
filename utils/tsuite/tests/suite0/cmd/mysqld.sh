#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=5.6.28

exclude_time_start
wget -nv http://downloads.mysql.com/archives/get/file/mysql-$V.tar.gz
exclude_time_end

decompress_gz mysql-$V.tar.gz | tar fx -
cd mysql-$V
cmake .
make
cd mysql-test
./mysql-test-run
